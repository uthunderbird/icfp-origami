import time
import os
import logging

from functools import wraps

import requests
import simplejson

cache_file_path = './data.json'

logging.basicConfig(filename='api.log', level=logging.DEBUG)

default_cache_structure = {}

_cache = None


def get_cache():
    global _cache
    if not _cache:
        s = None
        if os.path.isfile(cache_file_path):
            s = open(cache_file_path, 'r').read()
        if not s:
            set_cache(default_cache_structure.copy())
        else:
            _cache = simplejson.loads(s)
    return _cache


def save_cache():
    simplejson.dump(_cache, open(cache_file_path, 'w'))


def set_cache(obj):
    global _cache
    _cache = obj
    save_cache()

base_url = "http://2016sv.icfpcontest.org/api/"

hello_url = base_url + "hello"
snapshot_url = base_url + "snapshot/list"
blob_url = base_url + "blob/{hash}"
solution_submit_url = base_url + "solution/submit"

api_key = open('api_key').read()

session = requests.Session()
session.headers.update({'X-API-Key': api_key})

our_id = api_key.split('-', 1)[0]

state_holder = {'last_request': 0}


def apply_path(path, obj):
    for key in path:
        obj = obj[key]
    return obj


def cacher(**config):
    cache = get_cache()
    cache_mode = config.get('cache_mode') or 'permanent'
    def decorator(func):
        cache_field = config.get('cache_field') or func.__name__
        cache_params = config.get('cache_params', {})
        arg_index = cache_params.get('arg', 0)
        value_path = cache_params.get('value_path')

        def generate_getter(setter):

            def get_from_cache(value, *args, **kwargs):
                if value is None:
                    logging.info("There is no valid cache for {}. Executing request.".format(func.__name__))
                    value = setter(*args, **kwargs)
                else:
                    logging.info("Cache for {} was found. Using it.".format(func.__name__))
                return value

            if cache_mode == 'permanent':
                def getter(*args, **kwargs):
                    cached_value = get_from_cache(cache.get(cache_field), *args, **kwargs)
                    return cached_value
            elif cache_mode == 'permanent_by_arg':
                def getter(*args, **kwargs):
                    arg = args[arg_index]
                    if isinstance(arg, int):
                        arg = str(arg)
                    cached_value = get_from_cache(cache.get(cache_field, {}).get(arg), *args, **kwargs)
                    return cached_value
            elif cache_mode == 'valid_until_value':
                def getter(*args, **kwargs):
                    cached_obj = cache.get(cache_field)
                    if cached_obj:
                        value = apply_path(value_path, cached_obj) + cache_params['invalidation_time']
                        cached_value = get_from_cache(cached_obj if value > time.time() else None, *args, **kwargs)
                    else:
                        cached_value = get_from_cache(None, *args, **kwargs)
                    return cached_value
            return getter

        def generate_setter(requester):

            def save_to_cache(container, key, *args, **kwargs):
                container[key] = requester(*args, **kwargs)
                save_cache()
                return container[key]

            if cache_mode == 'permanent' or cache_mode == 'valid_until_value':
                def setter(*args, **kwargs):
                    value = save_to_cache(cache, cache_field, *args, **kwargs)
                    return value
            elif cache_mode == 'permanent_by_arg':
                def setter(*args, **kwargs):
                    if not cache_field in cache:
                        cache[cache_field] = {}
                    value = save_to_cache(cache[cache_field], args[arg_index], *args, **kwargs)
                    return value
            return setter
        getter = generate_getter(generate_setter(func))
        return getter
    return decorator


def api_endpoint():
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            timeout_required = time.time() < state_holder['last_request'] + 1 and state_holder['last_request'] != 0
            if timeout_required:
                time.sleep(state_holder['last_request'] + 1 - time.time())
            is_text = False
            if 'is_text' in kwargs:
                is_text = kwargs['is_text']
                del kwargs['is_text']
            resp = func(*args, **kwargs)
            state_holder['last_request'] = time.time()
            if resp.status_code == 200:
                if is_text:
                    return resp.text
                else:
                    return resp.json()
            else:
                logging.fatal(resp)
                logging.fatal(resp.text)
                raise ConnectionError()
        return wrapper
    return decorator

@cacher()
@api_endpoint()
def hello():
    return session.get(hello_url)


@cacher(cache_mode='valid_until_value', cache_params={'value_path': ['snapshots', -1, 'snapshot_time'],
                                                      'invalidation_time': 3600})
@api_endpoint()
def snapshot():
    return session.get(snapshot_url)


@cacher(cache_mode="permanent_by_arg", cache_params={'arg': 0})
@api_endpoint()
def blob(hash_):
    logging.info("Trying to get hash: {}".format(hash_))
    return session.get(blob_url.format(hash=hash_))


@api_endpoint()
def submit_solution(problem_id, solution):
    return session.post(solution_submit_url, data={'problem_id': problem_id, 'solution_spec': solution})


def status():
    snapshots = snapshot()['snapshots']
    for i in reversed(range(-len(snapshots), 0)):
        last_snapshot = snapshots[i]
        try:
            b = blob(last_snapshot['snapshot_hash'])
        except Exception as e:
            logging.fatal(e)
            logging.info("trying to use previous snapshot")
        else:
            return b


@cacher(cache_mode="permanent_by_arg", cache_params={'arg': 0})
def get_problem(i):
    return status()['problems'][i-1]


@cacher(cache_mode="permanent_by_arg", cache_params={'arg': 0})
def get_problem_spec(i):
    return blob(get_problem(i)['problem_spec_hash'], is_text=True)


def where_we_are():
    return list(map(lambda x: x['username'], status()['leaderboard'])).index(our_id) + 1

if __name__ == '__main__':
    import argparse
    import sys

    parser = argparse.ArgumentParser(prog='api_client.py')
    parser.add_argument('--loglevel', default='warning')
    subparsers = parser.add_subparsers(title="Subcommands", help='What do you want?')

    problem_parser = subparsers.add_parser('problem', help='Just do something with your problems, dude')
    problem_parser.add_argument('command', type=str, choices=['get_spec', 'get_info', 'submit_solution'])
    problem_parser.add_argument('id', type=int, choices=range(1, 2000))

    parser_b = subparsers.add_parser('leaderboard', help='Who is the best?')
    parser_b.add_argument('command', choices=['where_we_are'], help='DNIWE EBANOE')

    args = parser.parse_args()

    if hasattr(args, 'command'):
        if args.command == 'get_spec':
            print(get_problem_spec(args.id))
        elif args.command == 'get_info':
            print(get_problem(args.id))
        elif args.command == 'submit_solution':
            print(submit_solution(args.id, sys.stdin.read()))
        elif args.command == 'where_we_are':
            print(where_we_are())
