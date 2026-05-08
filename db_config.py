# db_config.py

CONNECTIONS = {
    "OLTP": {
        "url": "jdbc:oracle:thin:@//oltp_host:1521/oltp_service",
        "user": "oltp_user",
        "password": "oltp_password",
        "driver": "oracle.jdbc.OracleDriver"
    },
    "OLAP": {
        "url": "jdbc:oracle:thin:@//olap_host:1521/olap_service",
        "user": "olap_user",
        "password": "olap_password",
        "driver": "oracle.jdbc.OracleDriver"
    }
}