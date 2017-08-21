import os
from .base import DataImporter, fake_create_revision, get_reversion_manager
from openpyxl import load_workbook

class Excel(DataImporter):
    _cache = {}
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_defn = kwargs['file_defn']
        self.options = kwargs['options']
        self.stdout = kwargs['stdout']
        self.stderr = kwargs['stderr']
        self.debug_mode = kwargs['debug_mode']

    def process(self):
        filename = os.path.join(self.options['base_directory'], self.file_defn['file'])
        
        sheet_name = self.file_defn['excel']['sheet']
        lines = self.file_defn['excel'].get('lines', False)

        if lines:
            lines = sorted(map(int,lines))
            if len(lines) == 1:
                lines = [lines[0],lines[0]+1]

        model = self.get_model()

        self.stdout.write("importing file <%s> in as model <%s>"%(filename,model))
        
        print("starting loop")
        if filename in Excel._cache.keys():
            wb = Excel._cache.get(filename)
            print("using cache")
        else:
            print("starting loop")
            wb = load_workbook(filename, read_only=True)
            print("starting loop")
            ws = wb[sheet_name]
        print("starting loop")
        
        has_headers = self.file_defn['excel'].get('headers')
        if has_headers:
            pass #reader = csv.DictReader(imported_csv,delimiter=separator)
            headers = range(len(list(next(ws.rows))))
        else:
            # reader = csv.reader(imported_csv,delimiter=separator)  # creates the reader object
            headers = next(ws.rows) # get the headers


        failed = []
        success = []
        skipped = []
        print("starting loop")
        for i,row in enumerate(ws.rows):   # iterates the rows of the file in order
            if i == 0 and has_headers:
                continue
            row = dict(zip([h.value for h in headers],[c.value for c in row]))
            # print(i,row)

            if len(failed) > 100:
                self.stderr.write('something has gone terribly wrong.') 
                break
            if lines:
                if i < lines[0]:
                    continue
                elif i >= lines[1]:
                    break

            # model.objects.create()
            if i % 100 == 0:
                print('.')
            self.process_row(row, i)
        return self.success, self.skipped, self.failed

