# -*- coding: utf-8 -*-
"""
DOCSTRING TEMPLATE AND GUIDELINES
* Surround docstrings with 3 double quotes on either side.
* Write them for all public modules, functions, classes, and methods.
* Put the 3 double quotes that end a multiline docstring on a line by itself.
* Leave no blank lines between the closing double quotes and the start 
of the function.
* The width of docstring lines should be kept to 75 characters to facilitate 
easy reading in text terminals.

Multi-lined docstrings are used to further elaborate on the object.
All multi-lined docstrings should have the following parts:
* A one-line summary line (on the same line as the first 3 quotation marks)
* A blank line proceeding the summary
* Any further elaboration for the docstring
* Description of each required and optional parameter
* Another blank line before the closing 3 double quotes

The 'Example' section of the docstring is optional. If desired, include a one 
or two line example demonstrating how one might use this function in context 
or how the syntax might look. 

By including a valid docstring in your function, analysts can use the Python 
built-in function help() that prints out the object's docstring to the console
"""


def function_name(var1, var2, optional_var=False, optional_var2=0):
    """Summarize the function in one line.

    Several sentences providing an extended description. Refer to
    variables using back-ticks, e.g. `var`. 

    Parameters
    ----------
    var1 : str
        Description of variable. The type on the line above can either refer 
        to an actual Python type, like 'float' or 'str', or describe the type 
        of the variable in more detail, like `geodataframe'
    var2 : int
        Description of variable.
    optional_var : bool, optional (default False)
        Description of optional variable, with default value listed
    optional_var2 : int, optional (default 0)
        Description of optional variable, with default value listed

    Returns
    -------
    output_value : list
        Description of the object(s) returned by the function. 
        If the function returns nothing, and instead edits another object 
        directly, for example, write 'None' and describe the outcome.

    Example (optional)
    --------
	myoutput = function_name('blah', 10, optional_var=False, optional_var2=100)

    """
# ====================================================================


def quadratic(a, b, c):
    """Solve quadratic equation via the quadratic formula.

    A quadratic equation has the following form:
    ax**2 + bx + c = 0
    There always two solutions to a quadratic equation: x_1 & x_2.
    
    Parameters
    ----------
    a : int
        numeric constant. indicates the width and direction of the parabola
    b : int
        numeric constant. indicates the slope at the y-intercept
    c : int
        y-intercept of a parabola

    Returns
    -------
    x_1: int
        a description
    x_2: int
        a description
        
     Example
     -------
     X1, X2 = quadratic(5, 6, 1)       
    """
    x_1 = (- b+(b**2-4*a*c)**(1/2)) / (2*a)
    x_2 = (- b-(b**2-4*a*c)**(1/2)) / (2*a)

    return x_1, x_2

help(quadratic)
