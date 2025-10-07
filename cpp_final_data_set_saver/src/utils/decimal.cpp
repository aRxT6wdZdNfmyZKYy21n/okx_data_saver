#include "utils/decimal.h"
#include <sstream>
#include <iomanip>
#include <cmath>
#include <algorithm>

namespace okx {
namespace utils {

// Static constants
Decimal Decimal::ZERO = Decimal(static_cast<int64_t>(0));
Decimal Decimal::ONE = Decimal(static_cast<int64_t>(1));
Decimal Decimal::TEN = Decimal(static_cast<int64_t>(10));
Decimal Decimal::HUNDRED = Decimal(static_cast<int64_t>(100));

Decimal::Decimal(double val, int32_t precision) {
    (void)precision; // Suppress unused parameter warning
    value_ = boost_decimal(val);
}

Decimal::Decimal(const std::string& val, int32_t precision) {
    (void)precision; // Suppress unused parameter warning
    try {
        value_ = boost_decimal(val);
    } catch (const std::exception& e) {
        throw std::invalid_argument("Invalid decimal string: " + val);
    }
}

Decimal::Decimal(int64_t val, int32_t precision) {
    (void)precision; // Suppress unused parameter warning
    value_ = boost_decimal(val);
}

Decimal Decimal::operator+(const Decimal& other) const {
    return Decimal(value_ + other.value_);
}

Decimal Decimal::operator-(const Decimal& other) const {
    return Decimal(value_ - other.value_);
}

Decimal Decimal::operator*(const Decimal& other) const {
    return Decimal(value_ * other.value_);
}

Decimal Decimal::operator/(const Decimal& other) const {
    if (other.isZero()) {
        throw std::runtime_error("Division by zero");
    }
    return Decimal(value_ / other.value_);
}

bool Decimal::operator==(const Decimal& other) const {
    return value_ == other.value_;
}

bool Decimal::operator!=(const Decimal& other) const {
    return value_ != other.value_;
}

bool Decimal::operator<(const Decimal& other) const {
    return value_ < other.value_;
}

bool Decimal::operator<=(const Decimal& other) const {
    return value_ <= other.value_;
}

bool Decimal::operator>(const Decimal& other) const {
    return value_ > other.value_;
}

bool Decimal::operator>=(const Decimal& other) const {
    return value_ >= other.value_;
}

Decimal Decimal::operator-() const {
    return Decimal(-value_);
}

Decimal Decimal::operator+() const {
    return *this;
}

Decimal& Decimal::operator+=(const Decimal& other) {
    value_ += other.value_;
    return *this;
}

Decimal& Decimal::operator-=(const Decimal& other) {
    value_ -= other.value_;
    return *this;
}

Decimal& Decimal::operator*=(const Decimal& other) {
    value_ *= other.value_;
    return *this;
}

Decimal& Decimal::operator/=(const Decimal& other) {
    if (other.isZero()) {
        throw std::runtime_error("Division by zero");
    }
    value_ /= other.value_;
    return *this;
}

double Decimal::toDouble() const {
    return static_cast<double>(value_);
}

std::string Decimal::toString() const {
    std::ostringstream oss;
    oss << std::setprecision(MAX_PRECISION) << value_;
    return oss.str();
}

void Decimal::setPrecision(int32_t precision) {
    // No-op for compatibility - Boost.Multiprecision handles precision automatically
    (void)precision; // Suppress unused parameter warning
}

bool Decimal::isZero() const {
    return value_ == 0;
}

bool Decimal::isPositive() const {
    return value_ > 0;
}

bool Decimal::isNegative() const {
    return value_ < 0;
}

Decimal Decimal::abs() const {
    return Decimal(boost::multiprecision::abs(value_));
}

Decimal Decimal::round(int32_t precision) const {
    // For Boost.Multiprecision, we can use the built-in rounding
    // Convert to string with specified precision and back
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(precision) << value_;
    return Decimal(oss.str());
}

Decimal Decimal::sqrt() const {
    if (isNegative()) {
        throw std::runtime_error("Square root of negative number");
    }
    
    if (isZero()) {
        return ZERO;
    }
    
    return Decimal(boost::multiprecision::sqrt(value_));
}

Decimal Decimal::pow(int32_t exponent) const {
    if (exponent == 0) {
        return ONE;
    }
    
    return Decimal(boost::multiprecision::pow(value_, exponent));
}

Decimal Decimal::fromString(const std::string& str, int32_t precision) {
    return Decimal(str, precision);
}

Decimal Decimal::fromDouble(double val, int32_t precision) {
    return Decimal(val, precision);
}

Decimal Decimal::fromInt(int64_t val, int32_t precision) {
    return Decimal(val, precision);
}

std::ostream& operator<<(std::ostream& os, const Decimal& decimal) {
    os << decimal.toString();
    return os;
}

std::istream& operator>>(std::istream& is, Decimal& decimal) {
    std::string str;
    is >> str;
    decimal = Decimal::fromString(str);
    return is;
}

} // namespace utils
} // namespace okx