import random, uuid, time, json, sys, datetime, string
from psycopg2 import sql, Error, connect

# requires 'random' library
def get_random_words_dni(words_list, total=1):
    ran_words = []

    # enumerate over specified number of words
    while len(ran_words) < total:
        ran_words += [words_list[random.randint(0, len(words_list)-1)][0]]
    # return a string by joining the list of words
    return ' '.join(ran_words)

try:
    # declare a new PostgreSQL connection object
    conn = connect(
        dbname = "testbd",
        user = "postgres",
        host = "localhost",
        password = "123456",
    )

    cur = conn.cursor()
    print("\ncreated cursor object:", cur)

except Error as err:
    print("\npsycopg2 connect error:", err)
    conn = None
    cur = None

def create_postgres_json(size):

    # list to store JSON records
    records = []

    if cur != None:

        try:
            cur.execute('''
            SELECT dni
            FROM datos100k.persona;
            ''')
            usuario_dni = cur.fetchall()
            print('finished QUERY execution')

        except (Exception, Error) as error:
            print("\nexecute_sql() error:", error)
            conn.rollback()


    # random words to inject into records
    
    # iterate over the number of records being created
    for rec_id in range(size):

        # create a new record dict
        new_record = {}

        # input a value for each table column
        new_record['dni'] = get_random_words_dni(usuario_dni)
        new_record['contrasenia'] = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(8))

        # append the new record dict to the list
        while (True):
            encontrado = False
            for e in records:
                if e['dni'] == new_record['dni']:
                    encontrado = True
                    new_record['dni'] = get_random_words_dni(usuario_dni)
                if encontrado:
                    break
            if encontrado == False:
                break
        records += [ new_record ]

    # return the list of JSON records
    return records

def create_insert_records( json_array, table_name ):

    # get the columns for the JSON records
    columns = json_array[0].keys()

    # SQL column names should be lowercase using underscores instead of spaces/hyphens
    columns = [str(col).lower().replace(" ", "_") for col in columns]
    columns = [str(col).lower().replace("-", "_") for col in columns]
    # ("\ncolumns:", columns)

    # concatenate a string for the SQL 'INSERT INTO' statement
    sql_string = "INSERT INTO {}".format(table_name)
    sql_string = sql_string + " (" + ', '.join(columns) + ")\nVALUES "

    record_list = []
    for i, record in enumerate( json_array ):

        keys = record.keys()
        values = record.values()

        # use map() to cast all items in record list as a string
        #record = list(map(str, values))
        record = list(values)
        # (record)

        # fix the values in the list if needed
        for i, val in enumerate(record):

            if type(val) == str:
                if "'" in val:
                    # posix escape string syntax for single quotes
                    record[i] = "E'" + record[i].replace("'", "''") + "'"

        # cast record as string and remove the list brackets []
        record = str(record).replace("[", '')
        record = record.replace("]", '')

        # remove double quotes as well
        record = record.replace('"', '')

        # ..now append the records to the list
        record_list += [ record ]

    # enumerate() over the records and append to SQL string
    for i, record in enumerate(record_list):

        # use map() to cast all items in record list as a string
        #record = list(map(str, record))

        # append the record list of string values to the SQL string
        sql_string = sql_string + "(" + record + "),\n"

    # replace the last comma with a semicolon
    sql_string = sql_string[:-2] + ";"

    return sql_string

# ("\n")

# generate records for Postgres
quantity = 10**5
json_records = create_postgres_json(quantity)

# use the JSON library to convert JSON array into a Python string
json_records_str = json.dumps(json_records, indent=4, sort_keys=True, default=str)

# ("\nPostgres records JSON string:")
# (json_records_str)

# convert the string back to a dict (JSON) object
json_records = json.loads(json_records_str)

# allow the table name to be passed to the script
if len(sys.argv) > 1:
    table_name = '_'.join(sys.argv[1:])
else:
    # otherwise use a default table name
    table_name = 'datos100k.usuario'

# call the function to create INSERT INTO SQL string
sql_str = create_insert_records( json_records, table_name )

# ('\nsql_str:')
# (sql_str)


# save the generated Postgres records in a JSON file
with open('postgres-records.sql', 'w') as output_file:
    output_file.write(sql_str)

# only attempt to execute SQL if cursor is valid
if cur != None:
    try:
        sql_resp = cur.execute( sql_str )
        conn.commit()
        print('finished INSERT INTO execution')

    except (Exception, Error) as error:
        print("\nexecute_sql() error:", error)
        conn.rollback()

    # close the cursor and connection
    cur.close()
    conn.close()