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

Decimal::Decimal(double val, int32_t precision) 
    : precision_(precision) {
    if (precision < 0 || precision > MAX_PRECISION) {
        throw std::invalid_argument("Precision must be between 0 and " + std::to_string(MAX_PRECISION));
    }
    
    int64_t scale = getScaleFactor(precision);
    value_ = static_cast<int64_t>(std::round(val * scale));
}

Decimal::Decimal(const std::string& val, int32_t precision) 
    : precision_(precision) {
    if (precision < 0 || precision > MAX_PRECISION) {
        throw std::invalid_argument("Precision must be between 0 and " + std::to_string(MAX_PRECISION));
    }
    
    value_ = parseString(val, precision);
}

Decimal::Decimal(int64_t val, int32_t precision) 
    : precision_(precision) {
    if (precision < 0 || precision > MAX_PRECISION) {
        throw std::invalid_argument("Precision must be between 0 and " + std::to_string(MAX_PRECISION));
    }
    
    int64_t scale = getScaleFactor(precision);
    value_ = val * scale;
}

Decimal Decimal::operator+(const Decimal& other) const {
    auto [val1, val2] = normalizePrecision(other);
    return Decimal(val1 + val2, std::max(precision_, other.precision_), true);
}

Decimal Decimal::operator-(const Decimal& other) const {
    auto [val1, val2] = normalizePrecision(other);
    return Decimal(val1 - val2, std::max(precision_, other.precision_), true);
}

Decimal Decimal::operator*(const Decimal& other) const {
    int32_t resultPrecision = precision_ + other.precision_;
    int64_t resultValue = value_ * other.value_;
    
    // Scale down to maintain reasonable precision
    if (resultPrecision > MAX_PRECISION) {
        int64_t scaleDown = getScaleFactor(resultPrecision - MAX_PRECISION);
        resultValue /= scaleDown;
        resultPrecision = MAX_PRECISION;
    }
    
    return Decimal(resultValue, resultPrecision, true);
}

Decimal Decimal::operator/(const Decimal& other) const {
    if (other.isZero()) {
        throw std::runtime_error("Division by zero");
    }
    
    int32_t resultPrecision = std::max(precision_, other.precision_);
    int64_t scale = getScaleFactor(resultPrecision);
    
    // Scale up numerator to maintain precision
    int64_t scaledValue = value_ * scale;
    int64_t resultValue = scaledValue / other.value_;
    
    return Decimal(resultValue, resultPrecision, true);
}

bool Decimal::operator==(const Decimal& other) const {
    auto [val1, val2] = normalizePrecision(other);
    return val1 == val2;
}

bool Decimal::operator!=(const Decimal& other) const {
    return !(*this == other);
}

bool Decimal::operator<(const Decimal& other) const {
    auto [val1, val2] = normalizePrecision(other);
    return val1 < val2;
}

bool Decimal::operator<=(const Decimal& other) const {
    auto [val1, val2] = normalizePrecision(other);
    return val1 <= val2;
}

bool Decimal::operator>(const Decimal& other) const {
    auto [val1, val2] = normalizePrecision(other);
    return val1 > val2;
}

bool Decimal::operator>=(const Decimal& other) const {
    auto [val1, val2] = normalizePrecision(other);
    return val1 >= val2;
}

Decimal Decimal::operator-() const {
    return Decimal(-value_, precision_, true);
}

Decimal Decimal::operator+() const {
    return *this;
}

Decimal& Decimal::operator+=(const Decimal& other) {
    auto [val1, val2] = normalizePrecision(other);
    value_ = val1 + val2;
    precision_ = std::max(precision_, other.precision_);
    return *this;
}

Decimal& Decimal::operator-=(const Decimal& other) {
    auto [val1, val2] = normalizePrecision(other);
    value_ = val1 - val2;
    precision_ = std::max(precision_, other.precision_);
    return *this;
}

Decimal& Decimal::operator*=(const Decimal& other) {
    int32_t resultPrecision = precision_ + other.precision_;
    int64_t resultValue = value_ * other.value_;
    
    // Scale down to maintain reasonable precision
    if (resultPrecision > MAX_PRECISION) {
        int64_t scaleDown = getScaleFactor(resultPrecision - MAX_PRECISION);
        resultValue /= scaleDown;
        resultPrecision = MAX_PRECISION;
    }
    
    value_ = resultValue;
    precision_ = resultPrecision;
    return *this;
}

Decimal& Decimal::operator/=(const Decimal& other) {
    if (other.isZero()) {
        throw std::runtime_error("Division by zero");
    }
    
    int32_t resultPrecision = std::max(precision_, other.precision_);
    int64_t scale = getScaleFactor(resultPrecision);
    
    // Scale up numerator to maintain precision
    int64_t scaledValue = value_ * scale;
    int64_t resultValue = scaledValue / other.value_;
    
    value_ = resultValue;
    precision_ = resultPrecision;
    return *this;
}

double Decimal::toDouble() const {
    int64_t scale = getScaleFactor(precision_);
    return static_cast<double>(value_) / scale;
}

std::string Decimal::toString() const {
    std::ostringstream oss;
    
    if (precision_ == 0) {
        oss << value_;
    } else {
        int64_t scale = getScaleFactor(precision_);
        int64_t integerPart = value_ / scale;
        int64_t fractionalPart = std::abs(value_ % scale);
        
        oss << integerPart << ".";
        oss << std::setfill('0') << std::setw(precision_) << fractionalPart;
    }
    
    return oss.str();
}

void Decimal::setPrecision(int32_t precision) {
    if (precision < 0 || precision > MAX_PRECISION) {
        throw std::invalid_argument("Precision must be between 0 and " + std::to_string(MAX_PRECISION));
    }
    
    if (precision == precision_) {
        return;
    }
    
    int64_t currentScale = getScaleFactor(precision_);
    int64_t newScale = getScaleFactor(precision);
    
    if (precision > precision_) {
        // Increase precision
        value_ *= (newScale / currentScale);
    } else {
        // Decrease precision (round)
        value_ = (value_ + (currentScale / newScale) / 2) / (currentScale / newScale);
    }
    
    precision_ = precision;
}

Decimal Decimal::abs() const {
    return Decimal(std::abs(value_), precision_, true);
}

Decimal Decimal::round(int32_t precision) const {
    if (precision == precision_) {
        return *this;
    }
    
    Decimal result = *this;
    result.setPrecision(precision);
    return result;
}

Decimal Decimal::sqrt() const {
    if (isNegative()) {
        throw std::runtime_error("Square root of negative number");
    }
    
    if (isZero()) {
        return ZERO;
    }
    
    // Use double precision for calculation, then convert back
    double result = std::sqrt(toDouble());
    return Decimal::fromDouble(result, precision_);
}

Decimal Decimal::pow(int32_t exponent) const {
    if (exponent == 0) {
        return ONE;
    }
    
    // Use double precision for calculation, then convert back
    double result = std::pow(toDouble(), exponent);
    return Decimal::fromDouble(result, precision_);
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

std::pair<int64_t, int64_t> Decimal::normalizePrecision(const Decimal& other) const {
    int32_t maxPrecision = std::max(precision_, other.precision_);
    int64_t scale1 = getScaleFactor(maxPrecision - precision_);
    int64_t scale2 = getScaleFactor(maxPrecision - other.precision_);
    
    return {value_ * scale1, other.value_ * scale2};
}

int64_t Decimal::getScaleFactor(int32_t precision) {
    static const int64_t scaleFactors[] = {
        1LL,                    // 0
        10LL,                   // 1
        100LL,                  // 2
        1000LL,                 // 3
        10000LL,                // 4
        100000LL,               // 5
        1000000LL,              // 6
        10000000LL,             // 7
        100000000LL,            // 8
        1000000000LL,           // 9
        10000000000LL,          // 10
        100000000000LL,         // 11
        1000000000000LL,        // 12
        10000000000000LL,       // 13
        100000000000000LL,      // 14
        1000000000000000LL,     // 15
        10000000000000000LL,    // 16
        100000000000000000LL,   // 17
        1000000000000000000LL   // 18
    };
    
    if (precision < 0 || precision > MAX_PRECISION) {
        throw std::invalid_argument("Invalid precision: " + std::to_string(precision));
    }
    
    return scaleFactors[precision];
}

int64_t Decimal::parseString(const std::string& str, int32_t precision) {
    if (str.empty()) {
        throw std::invalid_argument("Empty string");
    }
    
    // Find decimal point
    size_t decimalPos = str.find('.');
    std::string integerPart, fractionalPart;
    
    if (decimalPos == std::string::npos) {
        integerPart = str;
        fractionalPart = "";
    } else {
        integerPart = str.substr(0, decimalPos);
        fractionalPart = str.substr(decimalPos + 1);
    }
    
    // Parse integer part
    int64_t result = 0;
    bool negative = false;
    
    if (!integerPart.empty() && integerPart[0] == '-') {
        negative = true;
        integerPart = integerPart.substr(1);
    }
    
    for (char c : integerPart) {
        if (c < '0' || c > '9') {
            throw std::invalid_argument("Invalid character in decimal string: " + std::string(1, c));
        }
        result = result * 10 + (c - '0');
    }
    
    // Parse fractional part
    int64_t scale = getScaleFactor(precision);
    int64_t fractionalValue = 0;
    
    for (size_t i = 0; i < fractionalPart.length() && i < static_cast<size_t>(precision); ++i) {
        char c = fractionalPart[i];
        if (c < '0' || c > '9') {
            throw std::invalid_argument("Invalid character in decimal string: " + std::string(1, c));
        }
        fractionalValue = fractionalValue * 10 + (c - '0');
    }
    
    // Scale up fractional part
    int64_t fractionalScale = getScaleFactor(precision - fractionalPart.length());
    fractionalValue *= fractionalScale;
    
    result = result * scale + fractionalValue;
    
    return negative ? -result : result;
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
