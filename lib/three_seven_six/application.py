import os, sys, logging, traceback, atexit, socket
from logging.handlers import SMTPHandler
import yaml
import three_seven_six
from three_seven_six.dbs import mysql


class Application:
    """
    Managers logging, error handling, pid-generation, etc.
    """

    def __init__(self, mysql=None):
        self.logger = self.initialize_logger()
        self._mysql = mysql

    @property
    def mysql(self):
        if self._mysql is None:
            self._mysql = mysql.Session()
        return self._mysql

    def run(self):
        try:
            self.runApplication()
        except:
            exc_info = sys.exc_info()
            self.logger.error('fatal error - aborting', exc_info=exc_info)

    def write_pid_file(self):
        path = "{0}/var/run/{1}.pid".format(three_seven_six.ROOT, self.app_name())
        dir = os.path.dirname(path)
        if not os.path.exists(dir):
            os.makedirs(dir)

        with open(path, 'w') as file:
            pid = str(os.getpid())
            file.write(pid)

            def delete_pid_file():
                if os.path.exists(path):
                    os.remove(path)

            atexit.register(delete_pid_file)

    def debug(self, message):
        self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self, message):
        self.logger.error(message)

    def initialize_logger(self):
        ROOT = os.path.normpath(os.path.realpath(__file__) + '/../../..')

        with open(three_seven_six.ROOT + '/etc/logging.yml') as file:
            full_config = yaml.load(file)
            config = full_config[three_seven_six.APP_ENV]

        most_verbose = min([STRING_TO_LOGGING_LEVEL[level] for level in config.values()])

        logger = logging.getLogger(self.app_name())
        logger.setLevel(most_verbose)

        log_path = '%s/log/%s.log' % (ROOT, self.app_name())
        file_handler = logging.FileHandler(log_path)
        formatter = logging.Formatter('%(asctime)s: %(levelname)s: %(message)s')
        file_handler.setFormatter(formatter)
        file_handler.setLevel(STRING_TO_LOGGING_LEVEL[config['file']])
        logger.addHandler(file_handler)
        if sys.stdout.isatty():
            handler = logging.StreamHandler()
            handler.setLevel(logging.DEBUG)
            logger.addHandler(handler)

        return logger


STRING_TO_LOGGING_LEVEL = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR
}


# ## enables using gmail
class TlsSMTPHandler(logging.handlers.SMTPHandler):
    def emit(self, record):
        """
        Emit a record.

        Format the record and send it to the specified addressees.
        """
        try:
            import smtplib
            import string  # for tls add this line

            try:
                from email.utils import formatdate
            except ImportError:
                formatdate = self.date_time
            port = self.mailport
            if not port:
                port = smtplib.SMTP_PORT
            smtp = smtplib.SMTP(self.mailhost, port)
            msg = self.format(record)
            msg = "From: %s\r\nTo: %s\r\nSubject: %s\r\nDate: %s\r\n\r\n%s" % (
                self.fromaddr,
                string.join(self.toaddrs, ","),
                self.getSubject(record),
                formatdate(), msg)
            if self.username:
                smtp.ehlo()  # for tls add this line
                smtp.starttls()  # for tls add this line
                smtp.ehlo()  # for tls add this line
                smtp.login(self.username, self.password)
            smtp.sendmail(self.fromaddr, self.toaddrs, msg)
            smtp.quit()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

