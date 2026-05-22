import re


def replace_special_characters(input_string):
    words = input_string.split(" ")
    camel_case_words = [word.capitalize().replace(" ", "") for word in words]
    camel_case_str = "".join(camel_case_words)
    pattern = re.compile("[^a-zA-Z0-9-_.]")
    result_string = pattern.sub("", camel_case_str)
    return result_string


def to_camel_case(namespace_format):
    words = namespace_format.split("/")
    camel_case_words = [word.title().replace(" ", "_") for word in words]
    camel_case_str = "/".join(camel_case_words)
    pattern = re.compile("[^a-zA-Z0-9-_./]")
    result_string = pattern.sub("", camel_case_str)
    return result_string
