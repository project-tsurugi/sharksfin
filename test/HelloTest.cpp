#include <gtest/gtest.h>

#include "sharksfin/sharksfin.h"

TEST(HelloTest, SayHello) {
  EXPECT_STREQ("Hello", sharksfin::say_hello().c_str());
}