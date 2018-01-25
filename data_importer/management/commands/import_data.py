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
from jinja2 import Environment, PackageLoader, select_autoescape


from contextlib import contextmanager
@contextmanager
def fake_create_revision(*args,**kwargs):
    yield

from data_importer import importers
from data_importer.importers.base import fake_create_revision, get_reversion_manager


env = Environment(
    autoescape=select_autoescape(['html', 'xml'])
)


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
        parser.add_argument('--arg',
            dest='meta_arguments',
            action='append',
            default=[],
            help='Arguments for the yaml'
        )
        parser.add_argument('-B','--base_directory',
            dest='base_directory',
            help='Directory to run file for'
        )

    def process_meta_args(self, meta_args):
        out = {}
        for arg in meta_args:
            key, val = arg.split(":",1)
            out[key] = val
        return out

    def handle(self, *args, **options):
        self.options = options
        self.debug_mode = self.options['debug']
        verbosity = options['verbosity']
        if verbosity > 1:
            self.stdout.write("Running {} command(s)".format(len(args)))


        self.meta_args = self.process_meta_args(options['meta_arguments'])
        details = None
        settings = open(options['action_file'], 'r').read()
        try:
            settings = env.from_string(settings).render(**self.meta_args)
            details = yaml.load(settings)
        except yaml.YAMLError as exc:
            print(exc)
            sys.exit(1)

        using_reversion, create_revision = get_reversion_manager(self.options['disable_reversion'])
        for i, file_defn in enumerate(details['files']):
            print("running", i)
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
            meta_args=self.meta_args
        )
        return kls.process()

    def process_csv_file(self, file_defn):
        from data_importer.importers import csv
        kls = csv.CSV(
            file_defn=file_defn, options=self.options,
            stdout=self.stdout, stderr=self.stderr,
            debug_mode=self.debug_mode,
            meta_args=self.meta_args
        )
        return kls.process()


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
        