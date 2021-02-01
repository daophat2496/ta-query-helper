def ParseJDBCUrl(jdbcurl) :
    url_segment = jdbcurl.split('/')
    host, port = url_segment[2].split(':')
    database = url_segment[3]
    return host, port, database
    