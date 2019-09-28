DEBUG = True

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
    }
}

INSTALLED_APPS = [
    'django_nose',
    'tests.testapp',
]

ROOT_URLCONF = 'tests.urls'

SECRET_KEY = "shh...it's a seakret"

CACHES = {
    'default': {
        'BACKEND': 'redis_cache.RedisCache',
    },
}
TEST_RUNNER = 'django_nose.NoseTestSuiteRunner'
MIDDLEWARE_CLASSES = tuple()
