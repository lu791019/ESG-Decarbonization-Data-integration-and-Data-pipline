
from elec_transfer.traning_models import tran_models
from services.mail_service import send_fail_mail, send_success_mail


def main():
    """for train models"""

    # green electricity
    tran_models()

if __name__ == '__main__':
    try:
        main()
        send_success_mail('training')
    except Exception as inst:

        send_fail_mail('training', inst)
