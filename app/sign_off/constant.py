from config import Config

DEVELOPER_EMAIL_LIST = ['felix_ye@wistron.com',
                        'viviana_fu@wistron.com',
                        'leo_jhuo@wistron.com',
                        'vincent_ku@wistron.com',
                        ]

signatureState = {
    "START": 2000,
    "ESTABLISHED": 2002,
    "NOTIFIED": 2003,
    "REJECTED": 2004,
    "APPROVED": 2005,

    "UNDEFINED": 0,
}

AUDIT_FLOW_STATE_CONFIG = {
    "ESTABLISHED": 'established',
    "NOTIFIED": 'notified',
    "REJECTED": 'rejected',
    "APPROVED": 'approved',
}


def system_base_url():
    if Config.FLASK_ENV == 'production':
        return 'https://decarb.k8sprd-whq.k8s.wistron.com'
    elif Config.FLASK_ENV == 'qas':
        return 'https://decarb.k8sqas-whq.k8s.wistron.com'
    else:
        return 'https://decarb.k8s-dev.k8s.wistron.com'
