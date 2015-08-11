from boto.s3.connection import S3Connection
import datetime
import simplejson as json
import requests
import os
import sys


class ModelServiceS3Sync(object):
    def __init__(self):
        self.models_path = ''
        self.bucket_name = ''
        self.aws_access_key_id = ""
        self.aws_secret_access_key = ""
        self.conn = None
        self.bucket = None
        self.ms_prefix = "http://0.0.0.0:8080"
        self.max_num_parameters = 16

    def with_models_path(self, models_path):
        self.models_path = models_path.split('/')[0] + '/'
        return self

    def with_bucket_name(self, bucket_name):
        self.bucket_name = bucket_name
        return self

    def with_aws_access_key_id(self, aws_id):
        self.aws_access_key_id = aws_id
        return self

    def with_aws_secret_access_key(self, aws_secret_key):
        self.aws_secret_access_key = aws_secret_key
        return self

    def with_model_service_connection(self, ms_string):
        self.ms_prefix = ms_string
        return self

    def init_connection(self):
        self.conn = S3Connection(self.aws_access_key_id,
                                 self.aws_secret_access_key)
        self.bucket = self.conn.get_bucket(self.bucket_name)
        return self

    def get_ms_model_list(self):
        return requests.get(self.ms_prefix + "/models").json()

    def get_model_names(self):
        ms_model_list = self.get_ms_model_list()
        ms_model_names = set([str(x) for x in ms_model_list.keys()])

        s3_model_prefixes = list(self.bucket.list(self.models_path, '/'))
        s3_model_names = set(map(
            lambda x: x.name.split('/')[1],
            s3_model_prefixes
        ))

        return (ms_model_names, s3_model_names)

    def load_new_model(self, model_key):
        print "Loading model: " + str(model_key)
        s3_feature_manager = self.bucket.get_key(self.models_path + model_key +
                                                 '/feature_manager')
        print s3_feature_manager
        if s3_feature_manager is not None:
            try:
                fm_dict = json.loads(s3_feature_manager.get_contents_as_string())
                # print "PUT " + self.ms_prefix + "/models/" + model_key
                put_response = requests.put(self.ms_prefix + "/models/" + model_key, json.dumps(fm_dict))
                print "RESPONSE: " + str(put_response.content)
            except Exception as local_exception:
                print local_exception

    def load_new_models(self):
        (ms_model_names, s3_model_names) = self.get_model_names()
        models_to_load = list(s3_model_names - ms_model_names)

        for m in models_to_load:
            self.load_new_model(m)

    def update_model(self, model_key):
        print "Updating model: " + str(model_key)
        current_parameter_set = set(
            self.get_ms_model_list()
                .get(str(model_key))
                .get('parameters', []))

        parameter_keys_list = [x.name for x in list(self.bucket.list(self.models_path + model_key + '/parameters/', '/'))]
        parameter_keys = [self.bucket.get_key(x) for x in parameter_keys_list]
        sorted_parameter_keys = sorted(
            parameter_keys,
            key=lambda x: datetime.datetime.strptime(x.last_modified, '%a, %d %b %Y %H:%M:%S %Z'),
            reverse=True)[:self.max_num_parameters]

        sorted_parameter_keys_to_update = filter(
            lambda x: x.name.split('/')[-1] not in current_parameter_set,
            sorted_parameter_keys
        )

        for p in reversed(sorted_parameter_keys_to_update):
            print "Loading parameters with key: " + str(p.name.split('/')[-1])
            put_response = requests.put(self.ms_prefix + '/models/' + model_key + '/' + str(p.name.split('/')[-1]), p.get_contents_as_string())
            print "RESPONSE: " + str(put_response.content)

    def update_current_models(self):
        (ms_model_names, s3_model_names) = self.get_model_names()
        models_to_update = list(s3_model_names & ms_model_names)

        for m in models_to_update:
            self.update_model(m)


if __name__ == '__main__':
    args = sys.argv
    if len(args) >= 3:
        ms_sync = ModelServiceS3Sync() \
            .with_bucket_name(args[1]) \
            .with_models_path(args[2]) \
            .with_aws_access_key_id(os.environ.get('AWS_ACCESS_KEY_ID', '')) \
            .with_aws_secret_access_key(os.environ.get('AWS_SECRET_ACCESS_KEY', ''))
        if len(args) == 4:
            ms_sync = ms_sync.with_model_service_connection(args[3])

        ms_sync.init_connection()

        ms_sync.load_new_models()
        ms_sync.update_current_models()
    else:
        print """\
        Arguments: <S3 bucket> <Model directory> [Model service URL]
            <S3 bucket> is the bucket name on S3
            <Model directory> is the path to the models stored withen the S3 bucket
            [Model service URL] is the URL of the model service server (defaults to 'http://0.0.0.0:8080')
        Set your AWS credentials with AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environmental variables\
        """
