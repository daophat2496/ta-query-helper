import sys
import psycopg2

def ParseJDBCUrl(jdbcurl) :
    url_segment = jdbcurl.split('/')
    host, port = url_segment[2].split(':')
    database = url_segment[3]
    return host, port, database

def ExecuteQuery(_connection, query) :
    try :
        cursor = _connection.cursor()
        cursor.execute(query)
        _connection.commit()
        if cursor.rowcount > 0:
            return cursor.fetchall()
    except Exception as e :
        if ("no results to fetch" in str(e).lower()) :
            return list()
        print(str(e))
        raise Exception("Error: {0}".format(e))
    else :
        return list()
    finally :
        cursor.close()

def main(argv) :
    db_host, db_port, db_user, db_password, db_dbname= argv[1:]

    detect_query_1 = open("./detect_query_1.sql", "r").read()
    clean_query_1 = open("./clean_query_1.sql", "r").read()

    detect_query_2 = open("./detect_query_2.sql", "r").read()
    clean_query_2 = open("./clean_query_2.sql", "r").read()

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
            
            connection = psycopg2.connect(host=host, port=port, user=db_user, password=db_password, database=database)

            detect_result_1 = ExecuteQuery(connection, detect_query_1)
            if len(detect_result_1) > 0 :
                print("Found client {0}, masterid {1}. Issue NULL roweffectivedate.Executing cleanup data in this database....".format(clientid, masterid))
                clean_result = ExecuteQuery(connection, clean_query_1)
                print("Done cleanup script.")
            
            detect_result_2 = ExecuteQuery(connection, detect_query_2)
            if len(detect_result_2) > 0 :
                print("Found client {0}, masterid {1}. Issue FactResult and DimLatestResult.Executing cleanup data in this database....".format(clientid, masterid))
                clean_result = ExecuteQuery(connection, clean_query_2)
                print("Done cleanup script.")

        except Exception as e:
            print("Error in client {0}, masterid {1}: {2}".format(clientid, masterid, e))
        
        finally:
            connection.close()
            

if __name__ == "__main__" :
    main(sys.argv)