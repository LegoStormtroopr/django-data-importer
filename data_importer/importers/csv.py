import os
import csv
from .base import DataImporter

class CSV(DataImporter):
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
        lines = self.file_defn['csv'].get('lines', False)

        if lines:
            lines = sorted(map(int,lines))
            if len(lines) == 1:
                lines = [lines[0],lines[0]+1]

        print("starting loop")
        separator = self.file_defn['csv']['separator']
        print("separator", separator)
        if separator in ['\\t','tab']:
            separator = '\t'

        with open(filename, 'r') as imported_csv:
            has_header = self.file_defn['csv'].get('header')
            
            if has_header:
                reader = csv.DictReader(imported_csv,delimiter=separator)
            else:
                reader = csv.reader(imported_csv,delimiter=separator)  # creates the reader object
                headers = reader.next() # get the headers

            print("starting loop")
            for i,row in enumerate(reader):   # iterates the rows of the file in order
                if i == 0 and has_header:
                    continue
                
                if not has_header:
                    row = dict(zip(headers,[c.value for c in row]))
                #print(i,row)
    
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

