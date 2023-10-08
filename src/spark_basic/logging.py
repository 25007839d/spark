# import logging
# LOG_FORMAT = '{lineno} *** {name} *** {asctime} *** {message}'
# logging.basicConfig(filename=r"C:\Users\KAJAL\Desktop\app.log",style='{', format=LOG_FORMAT)
#
# x=22
# if x<5:
#     print("value is >less")
# elif x>5:
#     logging.warning("value is large ")


# logging_example.py
#
# import logging
#
# # Create a custom logger
# logger = logging.getLogger(__name__)
# # Create handlers
# f_handler = logging.FileHandler('file.log')
# f_handler.setLevel(logging.ERROR())
# # Create formatters and add it to handlers
# f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# f_handler.setFormatter(f_format)
# logger.addHandler(f_handler)
#
#
#
# logger.warning('This is a warning')
# logger.error('This is an error')