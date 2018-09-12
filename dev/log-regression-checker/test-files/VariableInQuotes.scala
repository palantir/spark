// test variable in quotes
logError("blah $variable blah")
// test variable at the beginning and end and by itself
logError("blah $variable")
logError("$variable blah")
logError("$variable")

// nested variables
logError("${variable}")
logError("${{variable}}")
logError("${(variable)}")
logError("blah${variable}blah")

// arithmetic
logError("${var1 - var2 + (var3 / var4)}")

// quotes within var
logError("${var1 + "abc" + var2}")
