# Developer: Dhineshkumar Ramalingam


import argparse
import redis


def connect_redis(conn_dict):
    conn = None
    try:
        conn = redis.StrictRedis(
            host=conn_dict['host'],
            port=conn_dict['port'],
            db=conn_dict['db']
        )
    except Exception as e:
        print("Redis connection failed", e)
    finally:
        return conn


def conn_string_type(string):
    connection_format = '<host>:<port>/<db>'
    try:
        host, port_db = string.split(':')
        port, db = port_db.split('/')
        db = int(db)
    except ValueError:
        raise argparse.ArgumentTypeError('incorrect format, should be: %s' % connection_format)
    return {'host': host,
            'port': port,
            'db': db}


def migrate_redis(src, dst):
    for key in src.keys('*'):
        ttl = src.ttl(key)
        if ttl < 0:
            ttl = 0
        print('Dumping key: %s' % key)
        value = src.dump(key)
        print('Restoring key: %s' % key)
        try:
            dst.restore(key, ttl * 1000, value, replace=True)
        except redis.exceptions.ResponseError:
            print('Failed to restore key: %s' % key)
        except Exception as e:
            print('Failed to restore None key: %s' %key)
    return


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('source', type=conn_string_type)
    parser.add_argument('destination', type=conn_string_type)
    options = parser.parse_args()

    # Establishing connection to redis
    source_redis_conn = connect_redis(options.source)
    destination_redis_conn = connect_redis(options.destination)

    # Start migration process if both redis connections are working
    if source_redis_conn and destination_redis_conn:
        migrate_redis(source_redis_conn, destination_redis_conn)
    else:
        print('Source or Destination or both redis connection fails. checking...')
        if not source_redis_conn:
            print('Source redis connection failed')
        if not destination_redis_conn:
            print('Destination redis connection failed')


if __name__ == '__main__':
    run()
