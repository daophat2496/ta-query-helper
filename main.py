import sys
import psycopg2

def ParseJDBCUrl(jdbcurl) :
    url_segment = jdbcurl.split('/')
    host, port = url_segment[2].split(':')
    database = url_segment[3]
    return host, port, database

def ExecuteQuery(_db_info, query) :
    if len(_db_info) != 5 :
        print("wrong database argument")
        return
    host, port, user, password, dbname = _db_info

    try :
        connection = psycopg2.connect(host=host, port=port, user=user, password=password, database=dbname)
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        print(cursor.rowcount)
        if cursor.rowcount > 0:
            return cursor.fetchall()
    except Exception as e :
        print(str(e))
        raise Exception("Error: {0}".format(e))
    else :
        return list()
    finally :
        cursor.close()
        connection.close()

def ExecuteQueryWithCondition(_db_info, _conditional_query, _query) :
    if len(_db_info) != 5 :
        print("wrong database argument")
        return
    host, port, user, password, dbname = _db_info

    try :
        connection = psycopg2.connect(host=host, port=port, user=user, password=password, database=dbname)
        cursor = connection.cursor()
        cursor.execute(_conditional_query)
        
        if cursor.rowcount > 0:
            result = cursor.fetchall()
            cursor.execute(_query)
    except Exception as e :
        print(str(e))
        raise Exception("Error: {0}".format(e))
    else :
        return list()
    finally :
        cursor.close()
        connection.close()

def main(argv) :
    db_host, db_port, db_user, db_password, db_dbname= argv[1:]
    #query = "SELECT 1 FROM staging.testcases WHERE ssor != 'tosca' AND lastmodifiedtimestamp IS NULL LIMIT 1;"
    detect_query = open("./detect_query.sql", "r").read()
    clean_query = open("./clean_query.sql", "r").read()

    try :
        connection = psycopg2.connect(host=db_host, port=db_port, user=db_user, password=db_password, database=db_dbname)
        cursor = connection.cursor()
        cursor.execute("select * from metadata;")
        records = cursor.fetchall()
    except Exception as e :
        print("Error when get metadata: {0}".format(e))
        return
    finally :
        cursor.close()
        connection.close()

    for record in records :
        try :
            host, port, database = ParseJDBCUrl(record[5])
            clientid, masterid = record[1], record[3]
            result = ExecuteQuery((host, port, db_user, db_password, database), detect_query)

            if len(result) > 0 :
                print("Found client {0}, masterid {1}".format(clientid, masterid))

        except Exception as e:
            print("Error in client {0}, masterid {1}: {2}".format(clientid, masterid, e))
        else :
            

if __name__ == "__main__" :
    main(sys.argv)