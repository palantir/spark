logError(variable.field)
logError(var1 + var2 + "blah $var3" + var4)

// various combinations with stacktrace
logError(variable, e)
logError("blah", e)

// TODO: this actually returns "method" and "param" as two different variables...
logError(method(param))
