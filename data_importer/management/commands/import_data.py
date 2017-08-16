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
        parser.add_argument('-d','--date_format',
            action='store',
            dest='date_format',
            default='%Y-%m-%d',
            help='String representing the datetime format in a data file. Must be a conformant python strptime string.'
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
        if options['verbosity'] > 1:
            self.stdout.write("Running {} command(s)".format(len(args)))

        details = None
        with open(options['action_file'], 'r') as stream:
            try:
                details = yaml.load(stream)
            except yaml.YAMLError as exc:
                print(exc)
                sys.exit(1)

        # if not args or len(args) == 0:
        #     self.stdout.write(self.help)
        #     return
        # elif len(args) == 1:
        #     filename = args[0]
        # else:
        #     self.stderr.write("Wrong number of arguments")
        #     return

        for file_defn in details['files']:
            self.process_csv_file(file_defn)
            

    def process_csv_file(self, file_defn):
        requested_model = file_defn['model']
        
        filename = os.path.join(self.options['base_directory'], file_defn['file'])
        
        separator = file_defn['csv']['separator']
        verbosity = int(self.options['verbosity'])
        # lines = options['line_nos']
        lines = file_defn['csv'].get('lines', False)
        # update_keys = options['update_keys']
        # generic_map = {}
        
        if self.options['disable_reversion']:
            using_reversion = False
            create_revision = fake_create_revision
        else:
            try:
                import reversion as revisions
                using_reversion = True
                create_revision = revisions.create_revision
            except:
                using_reversion = False
                create_revision = fake_create_revision


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
            if file_defn.get('headers'):
                reader = csv.DictReader(imported_csv,delimiter=separator)
            else:
                reader = csv.reader(imported_csv,delimiter=separator)  # creates the reader object
                headers = reader.next() # get the headers

            # if generic_model is not None:
            #     if generic_model == 'FILENAME':
            #         path,fn = filename.rsplit('/',1)
            #         generic_model = '.'.join(fn.split('.')[:2])
            #         csv_field=headers[0]
            #         generic_key=headers[0]
            #         generic_field='content_object'
            #     elif len(generic_model.split(':')) == 4:
            #         generic_field,csv_field,generic_model,generic_key = generic_model.split(':',3)
            #     elif len(generic_model.split(':')) == 3:
            #         generic_field,csv_field,generic_model = generic_model.split(':',2)
            #         generic_key=headers[0]
            #     else:
            #         generic_field,generic_model = generic_model.split(':',1)
            #         csv_field='content_object'
            #         generic_key=headers[0]
            #     g_app_label,g_model = generic_model.lower().split('.',1)
            #     try:
            #         generic_model_type = ContentType.objects.get(app_label=g_app_label,model=g_model)
            #         generic_model = generic_model_type.model_class()
            #         generic_map[csv_field]=(generic_field,generic_model,generic_key)
            #     except ContentType.DoesNotExist:
            #         self.stderr.write("Model does not exist - %s"%generic_model)
            #         return 
            self.stdout.write("importing file <%s> in as model <%s>"%(filename,requested_model))
            start_time = time.time()

            failed = []
            success = []
            skipped = []
            with create_revision():#, transaction.atomic():
                for i,row in enumerate(reader):   # iterates the rows of the file in order
                    if len(failed) > 100:
                        self.stderr.write('something has gone terribly wrong.') 
                        break
                    if lines:
                        if i < lines[0]:
                            continue
                        elif i >= lines[1]:
                            break
                        
                    # special_starts = (  '_', # ignored column
                    #                     '+', # many-to-many id field
                    #                     '=', # assign value to a property after saving
                    #                     #'*', # Generics
                    #                 )
                    # if update_keys:
                    #     update_vals = dict(  [(clean(key),clean(val))
                    #                 for key,val in zip(headers,row)
                    #                 if key in update_keys])
                    # else:
                    #     update_vals = []
                        
                    # values = dict(  [(clean(key),clean(val))
                    #                 for key,val in zip(headers,row)
                    #                 if '.' not in key
                    #                     and not clean(key).startswith(special_starts)
                    #                     and not val == ''
                    #                     and clean(key) not in generic_map.keys()])
                    # rels = dict([   (clean(key),clean(val))
                    #                 for key,val in zip(headers,row)
                    #                 if '.' in key and not clean(key).startswith(special_starts)])
                    # many = dict([   (clean(key),clean(val))
                    #                 for key,val in zip(headers,row)
                    #                 if '.' not in key
                    #                     and clean(key).startswith('+')
                    #                     and not val == ''])
                    # funcs = dict([   (clean(key),clean(val))
                    #                 for key,val in zip(headers,row)
                    #                 if '.' not in key
                    #                     and clean(key).startswith('=')
                    #                     and not val == ''])
                    # generics = dict([   (clean(key),clean(val))
                    #                 for key,val in zip(headers,row)
                    #                 if '.' not in key
                    #                     and clean(key) in generic_map.keys()
                    #                     and not val == ''])

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

                        if file_defn.get('database', {}).get("key"):
                            # If the spec defines a lookup key to match this row against
                            keys = file_defn.get('database', {}).get("key")
                            lookup_vals = {}
                            for key in keys:
                                lookup_vals[key] = values.pop(key)
                            lookup_vals['defaults']=values
                            obj,created = model.objects.update_or_create(**lookup_vals)
                        else:
                            obj,created = model.objects.get_or_create(**values)

                        if self.debug_mode:
                            print(values)
    
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
        