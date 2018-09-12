// test all different types of errors
logError(s"Blah $variable blah")
logWarning(s"Blah $variable blah")
logInfo(s"Blah $variable blah")
logDebug(s"Blah $variable blah")

// test no variable name
logError(s"blah no variable")

// test not parseable
logInvalid(s"blah $variable blah")

// test all spaces before and after log lines
  logError(s"blah $variable blah")
      logError(s"blah $variable blah")

// test log line with error:
logError(s"log line $var", e)
