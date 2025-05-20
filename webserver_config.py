from flask_appbuilder.security.manager import AUTH_DB

print("webserver_config.py loaded")

AUTH_BACKENDS = ['airflow.www.security.FabAirflowSecurityManager']
AUTH_ROLE_PUBLIC = 'Admin'
WTF_CSRF_ENABLED = False
AUTH_TYPE = AUTH_DB
ENABLE_CORS = True
CORS_OPTIONS = {
    "supports_credentials": True,
    "origins": ["http://localhost:5173"]
}
