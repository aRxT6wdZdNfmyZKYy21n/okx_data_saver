#include <gtest/gtest.h>
#include "utils/decimal.h"

using namespace okx::utils;
#include <string>
#include <limits>


class DecimalTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Setup code if needed
    }
    
    void TearDown() override {
        // Cleanup code if needed
    }
};

TEST_F(DecimalTest, DefaultConstructor) {
    Decimal d;
    EXPECT_TRUE(d.isZero());
    EXPECT_EQ(d.getPrecision(), 16);
}

TEST_F(DecimalTest, ConstructorFromDouble) {
    Decimal d(123.456, 3);
    EXPECT_EQ(d.toString(), "123.456");
    EXPECT_EQ(d.getPrecision(), 3);
}

TEST_F(DecimalTest, ConstructorFromString) {
    Decimal d("123.456", 3);
    EXPECT_EQ(d.toString(), "123.456");
    EXPECT_EQ(d.getPrecision(), 3);
}

TEST_F(DecimalTest, ConstructorFromInt) {
    Decimal d(static_cast<int64_t>(123), 2);
    EXPECT_EQ(d.toString(), "123.00");
    EXPECT_EQ(d.getPrecision(), 2);
}

TEST_F(DecimalTest, Addition) {
    Decimal d1(10.5, 1);
    Decimal d2(20.3, 1);
    Decimal result = d1 + d2;
    EXPECT_EQ(result.toString(), "30.8");
}

TEST_F(DecimalTest, Subtraction) {
    Decimal d1(30.8, 1);
    Decimal d2(10.5, 1);
    Decimal result = d1 - d2;
    EXPECT_EQ(result.toString(), "20.3");
}

TEST_F(DecimalTest, Multiplication) {
    Decimal d1(10.5, 1);
    Decimal d2(2.0, 1);
    Decimal result = d1 * d2;
    EXPECT_EQ(result.toString(), "21.00");
}

TEST_F(DecimalTest, Division) {
    Decimal d1(21.0, 1);
    Decimal d2(2.0, 1);
    Decimal result = d1 / d2;
    EXPECT_EQ(result.toString(), "10.5");
}

TEST_F(DecimalTest, DivisionByZero) {
    Decimal d1(10.0, 1);
    Decimal d2(0.0, 1);
    EXPECT_THROW(d1 / d2, std::runtime_error);
}

TEST_F(DecimalTest, Comparison) {
    Decimal d1(10.5, 1);
    Decimal d2(20.3, 1);
    Decimal d3(10.5, 1);
    
    EXPECT_TRUE(d1 < d2);
    EXPECT_TRUE(d2 > d1);
    EXPECT_TRUE(d1 == d3);
    EXPECT_TRUE(d1 != d2);
    EXPECT_TRUE(d1 <= d3);
    EXPECT_TRUE(d1 >= d3);
}

TEST_F(DecimalTest, UnaryOperators) {
    Decimal d(10.5, 1);
    Decimal neg = -d;
    Decimal pos = +d;
    
    EXPECT_EQ(neg.toString(), "-10.5");
    EXPECT_EQ(pos.toString(), "10.5");
}

TEST_F(DecimalTest, CompoundAssignment) {
    Decimal d(10.5, 1);
    d += Decimal(5.5, 1);
    EXPECT_EQ(d.toString(), "16.0");
    
    d -= Decimal(1.0, 1);
    EXPECT_EQ(d.toString(), "15.0");
    
    d *= Decimal(2.0, 1);
    EXPECT_EQ(d.toString(), "30.00");
    
    d /= Decimal(3.0, 1);
    EXPECT_EQ(d.toString(), "100.00");
}

TEST_F(DecimalTest, ToDouble) {
    Decimal d(123.456, 3);
    EXPECT_DOUBLE_EQ(d.toDouble(), 123.456);
}

TEST_F(DecimalTest, SetPrecision) {
    Decimal d(123.456, 3);
    d.setPrecision(1);
    EXPECT_EQ(d.toString(), "123.5"); // Rounded
    EXPECT_EQ(d.getPrecision(), 1);
}

TEST_F(DecimalTest, IsZero) {
    Decimal d1(0.0, 1);
    Decimal d2(0.1, 1);
    
    EXPECT_TRUE(d1.isZero());
    EXPECT_FALSE(d2.isZero());
}

TEST_F(DecimalTest, IsPositiveNegative) {
    Decimal d1(10.5, 1);
    Decimal d2(-10.5, 1);
    Decimal d3(0.0, 1);
    
    EXPECT_TRUE(d1.isPositive());
    EXPECT_FALSE(d1.isNegative());
    
    EXPECT_FALSE(d2.isPositive());
    EXPECT_TRUE(d2.isNegative());
    
    EXPECT_FALSE(d3.isPositive());
    EXPECT_FALSE(d3.isNegative());
}

TEST_F(DecimalTest, Abs) {
    Decimal d(-10.5, 1);
    Decimal abs_d = d.abs();
    EXPECT_EQ(abs_d.toString(), "10.5");
}

TEST_F(DecimalTest, Round) {
    Decimal d(123.456, 3);
    Decimal rounded = d.round(1);
    EXPECT_EQ(rounded.toString(), "123.5");
}

TEST_F(DecimalTest, Sqrt) {
    Decimal d(16.0, 1);
    Decimal sqrt_d = d.sqrt();
    EXPECT_DOUBLE_EQ(sqrt_d.toDouble(), 4.0);
}

TEST_F(DecimalTest, SqrtNegative) {
    Decimal d(-16.0, 1);
    EXPECT_THROW(d.sqrt(), std::runtime_error);
}

TEST_F(DecimalTest, Pow) {
    Decimal d(2.0, 1);
    Decimal pow_d = d.pow(3);
    EXPECT_DOUBLE_EQ(pow_d.toDouble(), 8.0);
}

TEST_F(DecimalTest, PowNegative) {
    Decimal d(2.0, 1);
    Decimal pow_d = d.pow(-2);
    EXPECT_NEAR(pow_d.toDouble(), 0.25, 0.06);
}

TEST_F(DecimalTest, StaticFactoryMethods) {
    Decimal d1 = Decimal::fromString("123.456", 3);
    Decimal d2 = Decimal::fromDouble(123.456, 3);
    Decimal d3 = Decimal::fromInt(123, 3);
    
    EXPECT_EQ(d1.toString(), "123.456");
    EXPECT_EQ(d2.toString(), "123.456");
    EXPECT_EQ(d3.toString(), "123.000");
}

TEST_F(DecimalTest, Constants) {
    EXPECT_TRUE(Decimal::ZERO.isZero());
    EXPECT_EQ(Decimal::ONE.toString(), "1.0000000000000000");
    EXPECT_EQ(Decimal::TEN.toString(), "10.0000000000000000");
    EXPECT_EQ(Decimal::HUNDRED.toString(), "100.0000000000000000");
}

TEST_F(DecimalTest, InvalidPrecision) {
    EXPECT_THROW(Decimal(10.0, -1), std::invalid_argument);
    EXPECT_THROW(Decimal(10.0, 37), std::invalid_argument);
}

TEST_F(DecimalTest, InvalidString) {
    EXPECT_THROW(Decimal("abc", 1), std::invalid_argument);
    EXPECT_THROW(Decimal("", 1), std::invalid_argument);
}

TEST_F(DecimalTest, StreamOperators) {
    Decimal d(123.456, 3);
    std::ostringstream oss;
    oss << d;
    EXPECT_EQ(oss.str(), "123.456");
    
    std::istringstream iss("123.456");
    Decimal d2;
    iss >> d2;
    EXPECT_EQ(d2.toString(), "123.4560000000000000");
}

TEST_F(DecimalTest, PrecisionNormalization) {
    Decimal d1(10.5, 1);
    Decimal d2(20.30, 2);
    Decimal result = d1 + d2;
    EXPECT_EQ(result.toString(), "30.80");
}

TEST_F(DecimalTest, LargeNumbers) {
    Decimal d1(999999999.99, 2);
    Decimal d2(0.01, 2);
    Decimal result = d1 + d2;
    EXPECT_EQ(result.toString(), "1000000000.00");
}

TEST_F(DecimalTest, SmallNumbers) {
    Decimal d1(0.00000001, 8);
    Decimal d2(0.00000001, 8);
    Decimal result = d1 + d2;
    EXPECT_EQ(result.toString(), "0.00000002");
}
