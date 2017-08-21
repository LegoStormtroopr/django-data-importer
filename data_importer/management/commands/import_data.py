from django.core.exceptions import FieldDoesNotExist
from django.core.management.base import BaseCommand, CommandError
from django.contrib.contenttypes.models import ContentType
from django.db import transaction
from django.db.utils import IntegrityError
from django.db.models.fields.related import ForeignKey
from django.db.models.fields import DateField

import csv
import datetime
import time
import os, sys

import yaml

from contextlib import contextmanager
@contextmanager
def fake_create_revision(*args,**kwargs):
    yield

from data_importer import importers
from data_importer.importers.base import fake_create_revision, get_reversion_manager

class Command(BaseCommand):
    args = '<command string>'
    help = 'Allows for importing data'

    def add_arguments(self, parser):
        parser.add_argument('-D','--debug',
            action='store_true',
            dest='debug',
            default=False,
            help='Turn on debug'
        )
        parser.add_argument('-A','--action_file',
            action='store',
            dest='action_file',
            default=',',
            help='File that explains how to process a directory'
        )
        parser.add_argument('-R','--disable_reversion',
            action='store_true',
            dest='disable_reversion',
            default=False,
            help='If django-reversion is available on the system, disable using it for this upload.'
        )
        parser.add_argument('-B','--base_directory',
            dest='base_directory',
            help='Directory to run file for'
        )

    def handle(self, *args, **options):
        self.options = options
        self.debug_mode = self.options['debug']
        verbosity = options['verbosity']
        if verbosity > 1:
            self.stdout.write("Running {} command(s)".format(len(args)))

        details = None
        with open(options['action_file'], 'r') as stream:
            try:
                details = yaml.load(stream)
            except yaml.YAMLError as exc:
                print(exc)
                sys.exit(1)

        using_reversion, create_revision = get_reversion_manager(self.options['disable_reversion'])
        for file_defn in details['files']:
            start_time = time.time()

            with create_revision():#, transaction.atomic():

                if file_defn.get("csv", None):
                    processor = self.process_csv_file
                elif file_defn.get("excel", None):
                    processor = self.process_excel_file
                
                success, skipped, failed = processor(file_defn)

            elapsed_time = time.time() - start_time
            self.stdout.write("Summary:")
            if verbosity >=1:
                self.stdout.write("  Time taken: %.3f seconds"%elapsed_time)
            self.stdout.write("  Success: %s"%len(success))
            if skipped:
                self.stdout.write("  Skipped: %s"%len(skipped))
            if failed:
                self.stdout.write("  Failed: %s"%len(failed))
                self.stdout.write("  Failed on lines: %s"%str(failed))

    def process_excel_file(self, file_defn):
        from data_importer.importers import excel
        kls = excel.Excel(
            file_defn=file_defn, options=self.options,
            stdout=self.stdout, stderr=self.stderr,
            debug_mode=self.debug_mode,
        )
        return kls.process()
            # print(blah)

    def process_csv_file(self, file_defn):
        requested_model = file_defn['model']
        
        filename = os.path.join(self.options['base_directory'], file_defn['file'])
        
        separator = file_defn['csv']['separator']
        verbosity = int(self.options['verbosity'])
        # lines = options['line_nos']
        lines = file_defn['csv'].get('lines', False)
        # update_keys = options['update_keys']
        # generic_map = {}


        # if update_keys:
        #     update_keys = options['update_keys'].split(',')

        if lines:
            lines = sorted(map(int,lines))
            if len(lines) == 1:
                lines = [lines[0],lines[0]+1]
        # elif self.debug_mode:
        #     lines = [0,2]
            

        if requested_model is None:
            path,fn = filename.rsplit('/',1)
            requested_model,ext = fn.rsplit('.',1)

        if separator in ['\\t','tab']:
            separator = '\t'

        model = self.get_model(requested_model)

        with open(filename, 'r') as imported_csv:
            if file_defn['csv'].get('headers'):
                reader = csv.DictReader(imported_csv,delimiter=separator)
            else:
                reader = csv.reader(imported_csv,delimiter=separator)  # creates the reader object
                headers = reader.next() # get the headers

            self.stdout.write("importing file <%s> in as model <%s>"%(filename,requested_model))
            start_time = time.time()

            failed = []
            success = []
            skipped = []

            for i,row in enumerate(reader):   # iterates the rows of the file in order
                if len(failed) > 100:
                    self.stderr.write('something has gone terribly wrong.') 
                    break
                if lines:
                    if i < lines[0]:
                        continue
                    elif i >= lines[1]:
                        break
                if i % 5000 == 0:
                    print('.')

                values = {}
                # model.objects.create()
                try:
                    for f_name, f_details in file_defn['fields'].items():
                        # print(type(f_details))
                        
                        if type(f_details) not in [type({}), type([])]:
                            values[f_name] = row[f_details]
                        elif type(f_details) is type({}):
                            if f_details['type'] == 'lookup':
                                sub_model = self.get_model(f_details['model'])
                                try:
                                    values[f_name] = sub_model.objects.get(
                                        **dict([
                                            (f,row[v]) for f,v in f_details['fields'].items()
                                        ])
                                    )
                                except:
                                    print(f_details['not_found'])
                                    if f_details.get('not_found', None) == 'null':
                                        values[f_name] = None
                                    elif f_details.get('not_found', None) != 'skip':
                                        raise

                            if f_details['type'] == 'const_lookup':
                                sub_model = self.get_model(f_details['model'])
                                values[f_name] = sub_model.objects.get(
                                    **dict([
                                        (f,v) for f,v in f_details['fields'].items()
                                    ])
                                )

                            if f_details['type'] == 'coded':
                                mapping = f_details['choices']
                                default = mapping.pop('__unknown__', None)
                                values[f_name] = mapping.get(row[f_details['value']], default)
                                
                            if f_details['type'] == 'date':
                                val = row[f_details['value']]
                                try:
                                    values[f_name] = datetime.datetime.strptime(val,f_details['date_format']).date()
                                except:
                                    values[f_name] = None

                    if self.debug_mode:
                        print('vals', values)

                    if file_defn.get('database', {}).get("key"):
                        # If the spec defines a lookup key to match this row against
                        keys = file_defn.get('database', {}).get("key")
                        lookup_vals = {}
                        if type(keys) is str:
                            keys = [keys]
                        for key in keys:
                            lookup_vals[key] = values.pop(key)
                        lookup_vals['defaults']=values
                        obj,created = model.objects.update_or_create(**lookup_vals)
                    else:
                        obj,created = model.objects.get_or_create(**values)

                    if self.debug_mode:
                        print(created, obj)

                    if self.options.get('force_create', False):
                        created = True
                    if created:
                        success.append(i)
                        # for f,val in funcs.items():
                        #     f = f.lstrip('=')
                        #     setattr(obj,f,val)
                        # obj.save()
                    else:
                        if verbosity>=2:
                            self.stdout.write("Line %s - skipped"%i)
                        if verbosity==3:
                            self.stdout.write('%s'%row)
                        skipped.append(i)
                except Exception as e:
                    if verbosity >=2:
                        self.stderr.write("Line %s - %s"%(i,e))
                    if self.debug_mode:
                        raise
                    failed.append(i)
                # end transaction

        return success, skipped, failed

    def get_model(self, requested_model):
        try:
            app_label,model = requested_model.lower().split('.',1)
            model = ContentType.objects.get(app_label=app_label,model=model).model_class()
        except ContentType.DoesNotExist:
            self.stderr.write("Model does not exist - %s"%requested_model)
            return 
        return model

def clean(string):
    return string.decode('utf-8').strip('"').strip().replace("\"","")
        