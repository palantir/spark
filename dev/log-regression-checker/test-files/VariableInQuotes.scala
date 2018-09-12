// test variable in quotes
logError(s"blah $variable blah")
// test variable at the beginning and end and by itself
logError(s"blah $variable")
logError(s"$variable blah")
logError(s"$variable")

// nested variables
logError(s"${variable}")
logError(s"${{variable}}")
logError(s"${(variable)}")
logError(s"blah${variable}blah")

// arithmetic
logError(s"${var1 - var2 + (var3 / var4)}")

// quotes within var
logError(s"${var1 + "abc" + var2}")
logError(s"${var1 + s"abc$var2"} not_a_var")

// test literals
logError("$$ \) ${var} \{")

