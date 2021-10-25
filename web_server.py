import json
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)

HOST_NAME = "localhost"
SERVER_PORT = 4000

OK_CODE = 200
BAD_REQUEST_CODE = 400
NOT_FOUND_CODE = 404
METHOD_NOT_ALLOWED_CODE = 405
SERVER_ERROR_CODE = 500
JSON_CONTENT_TYPES = ['application/json']

key_value_dict = {}


class KeyValueDbHandler(BaseHTTPRequestHandler):

    def _validate_request_and_load_json(self):
        headers = dict(self.headers.items())
        is_valid = True
        accepts_json = True
        self.json_dict = {}

        if 'Content-Length' not in headers:
            is_valid = False

        content_len = int(headers.get('Content-Length', 0))
        content_type = headers.get('Content-Type')
        post_body = self.rfile.read(content_len)
        try:
            body = post_body.decode()
        except ValueError:
            body = ''

        accepted_types = headers.get('Accept', [])
        if accepted_types:
            if not isinstance(accepted_types, list):
                accepted_types = [accepted_types]
            accepts_json = any([
                acc_type in accepted_types for acc_type in JSON_CONTENT_TYPES + ['*/*']
            ])

        is_json_type = content_type and any(
            json_type in content_type for json_type in JSON_CONTENT_TYPES
        )

        if not accepts_json or not is_json_type:
            return False

        json_content = body
        try:
            self.json_dict = json.loads(json_content)
            if self.json_dict is None:
                self.json_dict = {}
        except ValueError:
            self.json_dict = {}

        return is_valid

    def validate_json_request(self, expected_params):
        """

        :param str | set | list | tuple expected_params: parameters expected to be present in the payload.
                                                        Can be a str for single param or
                                                        an iterable such as set, list, tuple for multiple params
        :returns: 2 elements tuple:
                     1. Validation passed: 1st element is True, 2nd element is the request's payload (from json to dict)
                     2. Validation failed: 1st element is False, 2nd element is the error payload (dict with `error` key)
        """
        is_valid = self._validate_request_and_load_json()
        if not is_valid:
            response_payload = {'error': 'Request should accept JSON and its body should be a JSON object. '
                                         '`Content-Length` header should also be specified'}
            return False, response_payload

        if not isinstance(expected_params, (set, list, tuple)):
            expected_params = [expected_params]
        if not all([param in self.json_dict for param in expected_params]):
            response_payload = {
                'error': 'Request is missing parameters. Expected: {}, Found: {}'.format(list(expected_params),
                                                                                         list(self.json_dict.keys()))
            }
            return False, response_payload

        return True, self.json_dict

    def send_json_response(self, payload):
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(bytes(json.dumps(payload), "utf-8"))

    def do_GET(self):
        try:
            if self.path.startswith('/get'):
                parsed_url = urlparse(self.path)
                params = parse_qs(parsed_url.query)
                if 'key' not in params:
                    payload = {
                        'error': 'Missing key parameter'
                    }
                    self.send_response(BAD_REQUEST_CODE, payload['error'])
                    self.send_json_response(payload)
                else:
                    key = params['key'][0]
                    if key not in key_value_dict:
                        payload = {
                            'error': 'Key `{}` does not exist in the database'.format(key)
                        }
                        self.send_response(NOT_FOUND_CODE, payload['error'])
                        self.send_json_response(payload)
                    else:
                        value = key_value_dict[key]
                        payload = {
                            'key': key,
                            'value': value
                        }
                        self.send_response(200)
                        self.send_json_response(payload)

            elif self.path.startswith('/set') or self.path.startswith('/delete'):
                payload = {
                    'error': 'Method Not Allowed. Using GET instead of POST'
                }
                self.send_response(METHOD_NOT_ALLOWED_CODE, payload['error'])
                self.send_json_response(payload)

            else:
                payload = {
                    'error': 'invalid path `{}`. Unavailable resource'.format(self.path)
                }
                self.send_response(NOT_FOUND_CODE, payload['error'])
                self.send_json_response(payload)

        except Exception as e:
            logging.exception(e)
            payload = {
                'error': "Internal Server Error"
            }
            self.send_response(SERVER_ERROR_CODE, payload['error'])
            self.send_json_response(payload)

    def do_POST(self):
        try:
            if self.path.startswith('/set'):
                is_valid, payload = self.validate_json_request({'key', 'value'})
                if not is_valid:
                    self.send_response(BAD_REQUEST_CODE, payload['error'])
                    self.send_json_response(payload)
                else:
                    key = payload['key']
                    value = payload['value']
                    if key in key_value_dict:
                        logging.info(
                            'Overriding existing key {} --> {} with new value: {}'.format(key, key_value_dict[key],
                                                                                          value)
                        )
                    else:
                        logging.info('Inserting new key-value pair: {} --> {}'.format(key, value))

                    key_value_dict[key] = value
                    self.send_response(OK_CODE)
                    self.send_json_response(payload)

            elif self.path.startswith('/delete'):
                is_valid, payload = self.validate_json_request('key')
                if not is_valid:
                    self.send_response(BAD_REQUEST_CODE, payload['error'])
                    self.send_json_response(payload)
                else:
                    key = payload['key']
                    if key in key_value_dict:
                        value = key_value_dict.pop(key)
                        logging.info('Deleted key-value pair: {} --> {}'.format(key, value))
                        self.send_response(OK_CODE)
                        self.send_json_response({
                            'key': key,
                            'value': value
                        })
                    else:
                        logging.info('Tried to delete non-existent key: {}'.format(key))
                        payload = {
                            'message': "'Key `{}` does not exist".format(key),
                        }
                        self.send_response(OK_CODE, payload['message'])
                        self.send_json_response(payload)

            elif self.path.startswith('/get'):
                payload = {
                    'error': 'Method Not Allowed. Using POST instead of GET'
                }
                self.send_response(METHOD_NOT_ALLOWED_CODE, payload['error'])
                self.send_json_response(payload)

            else:
                payload = {
                    'error': 'invalid path `{}`. Unavailable resource'.format(self.path)
                }
                self.send_response(NOT_FOUND_CODE, payload['error'])
                self.send_json_response(payload)

        except Exception as e:
            logging.exception(e)
            payload = {
                'error': "Internal Server Error"
            }
            self.send_response(SERVER_ERROR_CODE, payload['error'])
            self.send_json_response(payload)


if __name__ == "__main__":
    key_value_db_server = HTTPServer((HOST_NAME, SERVER_PORT), KeyValueDbHandler)
    print("### Key Value Database Server started http://%s:%s ###" % (HOST_NAME, SERVER_PORT))

    try:
        key_value_db_server.serve_forever()
    except KeyboardInterrupt:
        pass

    key_value_db_server.server_close()
    print("### Key Value Database Server stopped ###")
