package org.apache.spark.test;

import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

public final class HelloWorld {
  public static void main(String[] args) {
    List<String> strs = ImmutableList.of("Hello", "World");
    System.out.println(Joiner.on(",").join(strs));
  }
}
