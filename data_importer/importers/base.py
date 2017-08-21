from django.contrib.contenttypes.models import ContentType

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
        i = index
        model = self.get_model()
        verbosity = int(self.options.get('verbosity',1))
        values = {}
        try:
            for f_name, f_details in self.file_defn['fields'].items():

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
                print(values)

            if self.file_defn.get('database', {}).get("key"):
                # If the spec defines a lookup key to match this row against
                keys = self.file_defn.get('database', {}).get("key")
                lookup_vals = {}
                if type(keys) is str:
                    keys = [keys]
                for key in keys:
                    lookup_vals[key] = values.pop(key)
                lookup_vals['defaults']=values
                obj,created = model.objects.update_or_create(**lookup_vals)
            else:
                obj,created = model.objects.get_or_create(**values)

            if self.file_defn.get('after_create'):
                was_created = created
                this = obj
                row = row
                get_model = lambda x: self.get_model(x)

                script = self.file_defn.get('after_create').get('python')
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
                if verbosity>=2:
                    self.stdout.write("Line %s - skipped making model"%i)
                if verbosity==3:
                    self.stdout.write('%s'%row)
                self.skipped.append(i)
        except Exception as e:
            if verbosity >=2:
                self.stderr.write("Line %s - %s"%(i,e))
            if self.debug_mode:
                raise
            self.failed.append(i)
        # end transaction