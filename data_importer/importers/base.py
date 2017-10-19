from django.contrib.contenttypes.models import ContentType
import datetime

from contextlib import contextmanager
@contextmanager
def fake_create_revision(*args,**kwargs):
    yield

def get_reversion_manager(disable_reversion):
    if disable_reversion:
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
    return using_reversion, create_revision

class DataImporter(object):
    def __init__(self, *args, **kwargs):
        self.failed = []
        self.skipped = []
        self.success = []

        self.file_defn = kwargs['file_defn']
        self.options = kwargs['options']
        self.stdout = kwargs['stdout']
        self.stderr = kwargs['stderr']
        self.debug_mode = kwargs['debug_mode']
        self.verbosity = int(self.options.get('verbosity',1))

    def skip_row(self, i, row):
        if self.verbosity>=2:
            self.stdout.write("Line %s - skipped making model"%i)
        if self.verbosity==3:
            self.stdout.write('%s'%row)
        self.skipped.append(i)

    def get_model(self, requested_model=None):
        if requested_model is None:
            requested_model = self.file_defn['model']
        try:
            app_label,model = requested_model.lower().split('.',1)
            model = ContentType.objects.get(app_label=app_label,model=model).model_class()
        except ContentType.DoesNotExist:
            self.stderr.write("Model does not exist - %s"%requested_model)
            return None
        return model

    def process_row(self, row, index):
        if self.file_defn.get('models', None):
            self.import_defns = self.file_defn['models']
        else:
            self.import_defns = [self.file_defn]

        for import_defn in self.import_defns:
            model = self.get_model(import_defn['model'])
            # self.stdout.write("importing file <%s> in as model <%s>"%(filename,model))
            self.process_row_imports(row, index, import_defn)

    def process_row_imports(self, row, index, import_defn):
        i = index
        model = self.get_model(import_defn['model'])
        values = {}
        try:
            if import_defn.get("condition"):
                condition = import_defn["condition"]['python']
                if not eval(condition):
                    self.skip_row(i, row)
                    return
            for f_name, f_details in import_defn['fields'].items():

                if type(f_details) not in [type({}), type([])]:
                    values[f_name] = row[f_details]
                elif type(f_details) is type({}):
                    if f_details['type'] == 'null_is_blank':
                        val = row[f_details['field']]
                        if val is None:
                            val = ""
                        values[f_name] = val
                    if f_details['type'] == 'const':
                        values[f_name] = f_details['value']
                    if f_details['type'] == 'lookup':
                        sub_model = self.get_model(f_details['model'])
                        try:
                            lookups = {}
                            for f,v in f_details['fields'].items():
                                if type(v) is str:
                                    lookups[f] = row[v]
                                elif type(v) is dict:
                                    if v['type'] == "const":
                                        lookups[f] = v['value']
                            values[f_name] = sub_model.objects.get(
                                **lookups
                            )
                        except:
                            # print(f_details['not_found'])
                            if f_details.get('not_found', None) == 'null':
                                values[f_name] = None
                            elif f_details.get('not_found', None) == 'skip':
                                self.skip_row(i, row)
                                continue
                            else: #if f_details.get('not_found', None) != 'skip':
                                raise

                    if f_details['type'] == 'const_lookup':
                        sub_model = self.get_model(f_details['model'])
                        values[f_name] = sub_model.objects.get(
                            **dict([
                                (f,v) for f,v in f_details['fields'].items()
                            ])
                        )

                    if f_details['type'] == 'python':
                        script = f_details.get('code')
                        value = eval(script)
                        values[f_name] = value

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
                print(values)

            if import_defn.get('database', {}).get("key"):
                # If the spec defines a lookup key to match this row against
                keys = import_defn.get('database', {}).get("key")
                lookup_vals = {}
                if type(keys) is str:
                    keys = [keys]
                for key in keys:
                    lookup_vals[key] = values.pop(key)
                lookup_vals['defaults']=values
                obj,created = model.objects.update_or_create(**lookup_vals)
            else:
                obj,created = model.objects.get_or_create(**values)

            if import_defn.get('after_create'):
                was_created = created
                this = obj
                row = row
                get_model = lambda x: self.get_model(x)

                script = import_defn.get('after_create').get('python')
                if script:
                    try:
                        exec(script)
                    except:
                        pass


            if self.options.get('force_create', False):
                created = True
            if created:
                self.success.append(i)
            else:
                self.skip_row(i, row)
        except Exception as e:
            if self.verbosity >=2:
                self.stderr.write("Line %s - %s"%(i,e))
            if self.debug_mode:
                raise
            self.failed.append(i)
        # end transaction
