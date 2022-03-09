package com.hlf.batchchunk;

public class DontLikeItException extends Exception {
  public DontLikeItException(Integer integer) {
    super("Don't like " + integer);
  }
}
