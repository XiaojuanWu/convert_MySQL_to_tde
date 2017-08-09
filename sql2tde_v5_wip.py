import csv
import re
import sys
import os
import json
import datetime
import locale
import configparser
import argparse
from sqlalchemy import *
import MySQLdb.cursors
import pandas as pd
from tableausdk import Extract as tde
from tableausdk import *
from tableau_rest_api.tableau_rest_api import *

# Define type maps
schemaIniTypeMap = {
    'Bit': Type.BOOLEAN,
    'Byte': Type.INTEGER,
    'Short': Type.INTEGER,
    'Long': Type.INTEGER,
    'Integer': Type.INTEGER,
    'Single': Type.DOUBLE,
    'Double': Type.DOUBLE,
    'Date': Type.DATE,
    'DateTime': Type.DATETIME,
    'Text': Type.UNICODE_STRING,
    'Memo': Type.UNICODE_STRING
}

fieldSetterMap = {
    Type.BOOLEAN: lambda row, colNo, value: row.setBoolean(colNo, value.lower() == "true"),
    Type.INTEGER: lambda row, colNo, value: row.setInteger(colNo, int(value)),
    Type.DOUBLE: lambda row, colNo, value: row.setDouble(colNo, float(value)),
    Type.UNICODE_STRING: lambda row, colNo, value: row.setString(colNo, value),
    Type.CHAR_STRING: lambda row, colNo, value: row.setCharString(colNo, value),
    Type.DATE: lambda row, colNo, value: maketde.setDate(row, colNo, value),
    Type.DATETIME: lambda row, colNo, value: maketde.setDateTime(row, colNo, value)
}


class UpdateDateRange(object):
    def __init__(self):
        self.query_file = query_file
        self.query_folder = query_folder
        self.query_name = query_name
        self.new_start_date = new_end_date
        self.new_end_date = new_end_date

    def find_and_relplace_date(self, query_file, query_folder, new_start_date, new_end_date):
        # Set the regex pattern to find ####-##-##
        date_pattern = re.compile('(\d{4})[/.-](\d{2})[/.-](\d{2})')
        new_date_list = []

        sqlfile = open(self.query_file, 'rb')
        readfile = sqlfile.read()

        date_list = date_pattern.findall(readfile)
        for dt in date_list:
            year, month, day = dt
            newdates = datetime.date(int(year), int(month), int(day))
            new_date_list.append(newdates)

        # set the min/max dates as strings
        start_date = str(min(new_date_list))
        end_date = str(max(new_date_list))

        # check to see if the dates already match
        if start_date == new_start_date and end_date == new_end_date:
            print '### Date range already up to date'
            return False
        else:
            # Check to see if there is already a results.tde in the query directory
            if os.path.exists(query_folder + 'results.tde'):
                os.remove(query_folder + 'results.tde')
            # find the date strings to set up the replace
            find_min_date = re.compile(start_date)
            find_max_date = re.compile(end_date)

            replace_min = find_min_date.sub(new_start_date, readfile)
            replace_min_max = find_max_date.sub(new_end_date, replace_min)
            sqlfile.close()

            # Write the new dates into the existing file
            write_new_sql = open(self.query_file, 'wb')
            write_new_sql.write(replace_min_max)
            write_new_sql.close()
            print '### Date range updated'
            return True

    def valid_date(self, new_start_date, new_end_date, query_name):
        # Make sure the min/max dates are in YYYY-mm-dd format
        try:
            datetime.datetime.strptime(new_start_date, '%Y-%m-%d')
            datetime.datetime.strptime(new_end_date, '%Y-%m-%d')
            print '### Updating', query_name, 'for', new_start_date, 'to', new_end_date
        except ValueError:
            raise ValueError('Incorrect date or date format, start/end dates should be YYYY-mm-dd')
            sys.exit()

        if new_start_date > new_end_date:
            sys.exit('### Start date must be <= end date please re-enter range')


class PrepareData(object):
    """
    Class that will fold strings. See 'fold_string'.
    This object may be safely deleted or go out of scope when
    strings have been folded.
    It's intended to make sqlAlchemy use less memory for increased stability.
    """

    def __init__(self):
        self.unicode_map = {}
        self.query_file = query_file
        self.query_folder = query_folder

    def fold_string(self, s):
        # If s is not a string or unicode object, return it unchanged
        if not isinstance(s, basestring):
            return s

        # If s is already a string, then str() has no effect.
        # If s is Unicode, try and encode as a string and use intern.
        # If s is Unicode and can't be encoded as a string, this try
        # will raise a UnicodeEncodeError.
        try:
            return intern(str(s))
        except UnicodeEncodeError:
            # Fall through and handle s as Unicode
            pass

        # Look up the unicode value in the map and return
        # the object from the map. If there is no matching entry,
        # store this unicode object in the map and return it.
        t = self.unicode_map.get(s, None)
        if t is None:
            # Put s in the map
            t = self.unicode_map[s] = s
        return t

    def string_folding_wrapper(self, results):
        # Get the list of keys so that we build tuples with all
        # the values in key order.
        keys = results.keys()
        folder = PrepareData()
        for row in results:
            yield tuple(
                folder.fold_string(row[key])
                for key in keys
            )

    def read_sql(self, query_file, query_folder):
        # Create CSV to store query results
        sql_file = open(self.query_file, 'r')
        sql = sql_file.read()

        # Make sure to add in a config file
        print "### Running query for " + query_file.split('/')[2]
        engine = create_engine('mysql+mysqldb://read_tableau:ypxViOdFuVjP1Y0y3LPi@p-iproxy01.use01.plat.priv:11107/advertiser_3444_tetris', connect_args={'cursorclass': MySQLdb.cursors.SSCursor})
        conn = engine.connect()
        print "### Connecting to mysqldb..."
        engine.echo = False

        result = pd.read_sql_query(sql, engine)
        # print result

        outfile = open(self.query_folder + '/results.csv', 'wb')
        outcsv = csv.writer(outfile)
        print "### Fetching rows..."
        cursor = conn.execution_options(stream_results=True).execute(sql)
        sql_file.close()

        outcsv.writerow(result.keys())
        data = cursor.fetchall()
        outcsv.writerows(data)
        outfile.close()

        df = pd.DataFrame(self.string_folding_wrapper(cursor))
        df.to_csv('result2.csv')


class GenerateTde(object):
    def __init__(self, query_name):
        self.query_name = query_name
        self.query_file = query_file
        self.query_folder = query_folder

    def read_sql(self, query_file, query_folder):

        # Create CSV to store query results
        sql_file = open(self.query_file, 'r')
        sql = sql_file.read()

        # Make sure to add in a config file
        print "### Running query for " + query_file.split('/')[2]
        engine = create_engine('mysql+mysqldb://read_tableau:ypxViOdFuVjP1Y0y3LPi@p-iproxy01.use01.plat.priv:11107/advertiser_3444_tetris', connect_args={'cursorclass': MySQLdb.cursors.SSCursor})
        conn = engine.connect()
        print "### Connecting to mysqldb..."
        engine.echo = False

        result = pd.read_sql_query(sql, engine)

        # print result
        outfile = open(self.query_folder + '/results.csv', 'wb')
        outcsv = csv.writer(outfile)
        print "### Fetching rows..."
        cursor = conn.execution_options(stream_results=True).execute(sql)
        sql_file.close()
        outcsv.writerow(result.keys())

        # dump rows
        outcsv.writerows(cursor.fetchall())
        outfile.close()

    def setDate(self, row, colNo, value):
        d = datetime.datetime.strptime(value, "%Y-%m-%d")
        row.setDate(colNo, d.year, d.month, d.day)

    def setDateTime(self, row, colNo, value):
        if(value.find(".") != -1):
            d = datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
        else:
            d = datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        row.setDateTime(colNo, d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond / 100)

    def create_tde(self, query_folder):
        # Identify CSV input
        csvFile = self.query_folder + 'results.csv'
        # Open CSV file
        csvReader = csv.reader(open(csvFile, 'rb'), delimiter=',', quotechar='"')
        # Read schema.ini file, if it exists
        schemaFile = self.query_folder + "schema.ini"
        hasHeader = True
        colNames = []
        colTypes = []
        locale.setlocale(locale.LC_ALL, '')
        colParser = re.compile(r'(col)(\d+)', re.IGNORECASE)
        schemaIni = configparser.ConfigParser()
        schemaIni.read(schemaFile)
        schemaIni.sections()
        for item in schemaIni.items('default'):
            name = item[0]
            value = item[1]
            if name == "colnameheader":
                hasHeader = value == "True"
            m = colParser.match(name)
            if not m:
                continue
            colName = m.groups()[0]
            colNo = int(m.groups()[1]) - 1
            parts = value.split(' ')
            name = parts[0]
            try:
                type = schemaIniTypeMap[parts[1]]
            except KeyError:
                type = Type.UNICODE_STRING
            while colNo >= len(colNames):
                colNames.append(None)
                colTypes.append(Type.UNICODE_STRING)
            colNames[colNo] = name
            colTypes[colNo] = type

        # Create TDE output
        tdefile = csvFile.split('.')[0] + ".tde"
        print "### Creating extract:", tdefile
        with tde.Extract(tdefile) as extract:
            table = None  # set by createTable
            tableDef = None

            # Define createTable function
            def createTable(line):
                if line:
                    # append with empty columns so we have the same number of columns as the header row
                    while len(colNames) < len(line):
                        colNames.append(None)
                        colTypes.append(Type.UNICODE_STRING)
                    # write in the column names from the header row
                    colNo = 0
                    for colName in line:
                        colNames[colNo] = colName
                        colNo += 1

                # for any unnamed column, provide a default
                for i in range(0, len(colNames)):
                    if colNames[i] is None:
                        colNames[i] = 'F' + str(i + 1)

                # create the schema and the table from it
                if extract.hasTable('Extract'):
                    table = extract.openTable('Extract')
                    tableDef = table.getTableDefinition()
                else:
                    tableDef = tde.TableDefinition()
                    for i in range(0, len(colNames)):
                        tableDef.addColumn(colNames[i], colTypes[i])
                    table = extract.addTable("Extract", tableDef)
                return table, tableDef

            # Read the table
            print "### Adding rows to .tde..."
            rowNo = 0
            for line in csvReader:
                # Create the table upon first row (which may be a header)
                if table is None:
                    table, tableDef = createTable(line if hasHeader else None)
                    if hasHeader:
                        continue

                # We have a table, now write a row of values
                row = tde.Row(tableDef)
                colNo = 0
                for field in line:
                    if(colTypes[colNo] != Type.UNICODE_STRING and field == ""):
                        row.setNull(colNo)
                    else:
                        fieldSetterMap[colTypes[colNo]](row, colNo, field)
                    colNo += 1
                table.insert(row)

                # Output progress line
                rowNo += 1
                if rowNo % 10000 == 0:
                    print "###", "\b" * 32 + locale.format("%d", rowNo, grouping=True), "rows inserted",

            # Terminate progress line
            if rowNo >= 10000:
                print  # terminate line
            print "### All rows added"

    def publish_tde(query_name, query_folder):
        # REST api upload process
        # u_startTime = time.time()
        server = 'http://p-p3tableaum01.use01.plat.priv'
        username = '*****'
        password = '*****'
        rest = TableauRestApi(server, username, password, site_content_url='BetaTest2')
        logger = Logger('publish.log')
        rest.enable_logging(logger)
        rest.signin()

        try:
            print "### Publishing to server..."
            template_ds = query_folder + 'results.tde'
            folder = query_folder
            ds_name = folder.split('/')[1]
            rest_ds_proj_luid = rest.query_project_luid_by_name('default')
            # Publishing up my first data source to Test, from disk
            rest.publish_datasource(template_ds, ds_name, rest_ds_proj_luid, overwrite=True, connection_password=password, connection_username=username)
            print "### successfully uploaded", ds_name
        except:
            print "### the extract does not exist"

        # u_runTime = time.time() - u_startTime
        # print "Elapsed:", locale.format("%.2f", u_runTime), "seconds"


if __name__ == '__main__':
    query_config_file = 'datasource.json'
    datasource_count = 0

    parser = argparse.ArgumentParser(description='Generates and uploads .tde files from sql queries')
    parser.add_argument('--start', '-s', action='store', dest='startdate', help='Start date for date range')
    parser.add_argument('--end', '-e', action='store', dest='enddate', help='End date for the date range')
    parser.add_argument('--module', '-m', action='store', dest='module', help='Module to pull datasources from')
    parser_results = parser.parse_args()
    new_start_date = parser_results.startdate
    new_end_date = parser_results.enddate
    module = parser_results.module
    if module is null or module not in ['Engagement', 'LTV', 'UA']:
        NameError('Enter one of the following modules:', ['Engagement', 'LTV', 'UA'])
    if new_start_date is null:
        ValueError('Enter a start date - dates should be YYYY-mm-dd')
    elif new_start_date is null and new_end_date is null:
        ValueError('Enter at least start date - dates should be YYYY-mm-dd')
    if new_end_date is null:
        print '### No end date set, using today:', datetime.datetime.now().date()
        new_end_date = datetime.datetime.now().date()

    with open(query_config_file, 'rb') as datasource_list:
        readfile = datasource_list.read()
        reader = json.loads(readfile)
        beginTime = time.time()

        for row in reader[module]:
            datasource_count += 1
            startTime = time.time()
            query_name = row
            query_folder = 'query-files/' + query_name + '/'
            query_file = 'query-files/' + query_name + '/' + query_name + '.sql'

            udr = UpdateDateRange()
            readdata = PrepareData()
            maketde = GenerateTde(query_name)

            udr.valid_date(new_start_date, new_end_date, query_name)
            datecheck = udr.find_and_relplace_date(query_file, query_folder, new_start_date, new_end_date)

            if datecheck is True:
                readdata.read_sql(query_file, query_folder)

                q_endTime = time.time()
                q_runTime = q_endTime - startTime

                e_startTime = time.time()
                maketde.create_tde(query_folder)
                e_runTime = time.time() - e_startTime

                u_startTime = time.time()
                maketde.publish_tde(query_folder)
                u_runTime = time.time() - u_startTime

                readoutfile = open(query_folder + '/results.csv', 'r')
                rowcount = sum(1 for row in readoutfile) - 1
                readoutfile.close()
            else:
                q_endTime = time.time()
                q_runTime = q_endTime - startTime

                e_startTime = time.time()
                e_runTime = time.time() - e_startTime

                u_startTime = time.time()
                u_runTime = time.time() - u_startTime
                print '### Returned results from last execution'
                readoutfile = open(query_folder + '/results.csv', 'r')
                rowcount = sum(1 for row in readoutfile) - 1
                readoutfile.close()

            # Output elapsed time
            print "----------------------------------------------------------------------"
            print query_name, "Elapsed Time Summary:"
            print "Step 1: Run Query\t", locale.format("%.2f", q_runTime), "seconds"
            print "Step 2: Generate .tde\t", locale.format("%.2f", e_runTime), "seconds"
            print "Step 3: Upload\t\t", locale.format("%.2f", u_runTime), "seconds"
            print "----------------------------------------------------------------------"
            print "Elapsed:\t\t", locale.format("%.2f", time.time() - startTime), "seconds"
            print "Rows Returned:\t\t", rowcount
            print "----------------------------------------------------------------------"
        print "Session Totals:"
        print "----------------------------------------------------------------------"
        print "Total Elapsed:\t\t", locale.format("%.2f", time.time() - beginTime), "seconds"
        print "Files Uploaded:\t\t", datasource_count
        print "Average:\t\t", locale.format("%.2f", (time.time() - beginTime) / datasource_count), "sec per file"
        print "----------------------------------------------------------------------"
