package com.example.p1

class MainSpec extends munit.FunSuite {
  val a = 1
  val b = 2
  assertEquals(a, a)
  assert(b > a)
}
