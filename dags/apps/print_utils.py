import inspect
import functools


def print_logging_info(file_path: str = "", class_name: str = "", function_name: str = "", current_status: str = "", params_args: dict = {}, status_message: str = ""):
    function_or_method = ""
    if class_name:
        if params_args and "self" in params_args:
            del params_args["self"]
        function_or_method += f"Class: {class_name}\nMethod: {function_name}"
    else:
        function_or_method += f"Function: {function_name}"
    formatted_args = "\n\t".join(f"{key}: {value}" for key, value in params_args.items())

    print(f"\n{'*' * 100}\nFile: {file_path}\n{function_or_method}\nCurrent Status: {current_status}\nArguments:\n\t{formatted_args}\nMessage: {status_message}\n{'*' * 100}")


def print_logging_info_decorator(func=None, *, redacted_params=None):
    if func is None:
        return functools.partial(print_logging_info_decorator, redacted_params=redacted_params)

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        class_name = ""
        if "self" in kwargs:
            class_name = kwargs["self"].__class__.__name__
        function_name = func.__name__
        file_path = inspect.getfile(func)
        kwargs_copy = kwargs.copy()
        if redacted_params is not None:
            for param in redacted_params:
                if param in kwargs_copy:
                    kwargs_copy[param] = "[REDACTED]"
        print_logging_info(
            file_path=file_path,
            class_name=class_name,
            function_name=function_name,
            current_status="START",
            params_args=kwargs_copy,
            status_message="Function execution started."
        )
        result = func(*args, **kwargs)
        print_logging_info(
            file_path=file_path,
            class_name=class_name,
            function_name=function_name,
            current_status="END",
            params_args=kwargs_copy,
            status_message="Function execution completed."
        )
        return result
    return wrapper
