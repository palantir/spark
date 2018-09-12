// test all different types of errors
logError("Blah $variable blah")
logWarning("Blah $variable blah")
logInfo("Blah $variable blah")
logDebug("Blah $variable blah")

// test no variable name
logError("blah no variable")

// test not parseable
logInvalid("blah $variable blah")

// test all spaces before and after log lines
  logError("blah $variable blah")
      logError("blah $variable blah")
