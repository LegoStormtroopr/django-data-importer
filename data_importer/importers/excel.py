import os
from .base import DataImporter, fake_create_revision, get_reversion_manager
from openpyxl import load_workbook

class Excel(DataImporter):
    _cache = {}
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def process(self):
        filename = os.path.join(self.options['base_directory'], self.file_defn['file'])
        sheet_name = self.file_defn['excel']['sheet']
        lines = self.file_defn['excel'].get('lines', False)

        if lines:
            lines = sorted(map(int,lines))
            if len(lines) == 1:
                lines = [lines[0],lines[0]+1]

        print("starting loop")

        
        if filename in Excel._cache.keys():
            wb = Excel._cache.get(filename)
            print("using cache")
        else:
            print("starting loop")
            wb = load_workbook(filename, read_only=True)
            Excel._cache[filename] = wb
            print("starting loop")
        ws = wb[sheet_name]

        has_header = self.file_defn['excel'].get('header')
        print("has header", has_header)
        if has_header:
            headers = [h.value for h in next(ws.rows)] # get the headers
        else:
            headers = range(len(list(next(ws.rows))))

        print("starting loop")
        for i,row in enumerate(ws.rows):   # iterates the rows of the file in order
            if i == 0 and has_header:
                continue
            row = dict(zip(headers,[c.value for c in row]))
            # print(i,row)

            if len(self.failed) > 100:
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

