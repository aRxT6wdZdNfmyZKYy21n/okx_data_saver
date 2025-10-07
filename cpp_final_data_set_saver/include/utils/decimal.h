#pragma once

#include <string>
#include <cstdint>
#include <stdexcept>
#include <iostream>
#include <boost/multiprecision/cpp_dec_float.hpp>

namespace okx {
namespace utils {

/**
 * @brief High-precision decimal class for financial calculations using Boost.Multiprecision
 * 
 * This class provides decimal arithmetic using Boost.Multiprecision cpp_dec_float,
 * avoiding floating-point precision issues in financial calculations.
 */
class Decimal {
private:
    using boost_decimal = boost::multiprecision::cpp_dec_float_50;
    boost_decimal value_;
    
    static constexpr int32_t DEFAULT_PRECISION = 16;
    static constexpr int32_t MAX_PRECISION = 50; // cpp_dec_float_50 supports up to 50 digits
    
public:
    /**
     * @brief Default constructor (creates 0.0)
     */
    Decimal() : value_(0) {}
    
    /**
     * @brief Constructor from double
     * @param val Double value to convert
     * @param precision Number of decimal places (ignored, kept for compatibility)
     */
    explicit Decimal(double val, int32_t precision = DEFAULT_PRECISION);
    
    /**
     * @brief Constructor from string
     * @param val String representation of decimal number
     * @param precision Number of decimal places (ignored, kept for compatibility)
     */
    explicit Decimal(const std::string& val, int32_t precision = DEFAULT_PRECISION);
    
    /**
     * @brief Constructor from integer
     * @param val Integer value
     * @param precision Number of decimal places (ignored, kept for compatibility)
     */
    Decimal(int64_t val, int32_t precision = DEFAULT_PRECISION);
    
    /**
     * @brief Copy constructor
     */
    Decimal(const Decimal& other) = default;
    
    /**
     * @brief Assignment operator
     */
    Decimal& operator=(const Decimal& other) = default;
    
    // Arithmetic operators
    Decimal operator+(const Decimal& other) const;
    Decimal operator-(const Decimal& other) const;
    Decimal operator*(const Decimal& other) const;
    Decimal operator/(const Decimal& other) const;
    
    // Comparison operators
    bool operator==(const Decimal& other) const;
    bool operator!=(const Decimal& other) const;
    bool operator<(const Decimal& other) const;
    bool operator<=(const Decimal& other) const;
    bool operator>(const Decimal& other) const;
    bool operator>=(const Decimal& other) const;
    
    // Unary operators
    Decimal operator-() const;
    Decimal operator+() const;
    
    // Compound assignment operators
    Decimal& operator+=(const Decimal& other);
    Decimal& operator-=(const Decimal& other);
    Decimal& operator*=(const Decimal& other);
    Decimal& operator/=(const Decimal& other);
    
    /**
     * @brief Convert to double
     * @return Double representation
     */
    double toDouble() const;
    
    /**
     * @brief Convert to string
     * @return String representation
     */
    std::string toString() const;
    
    /**
     * @brief Get precision (always returns MAX_PRECISION for compatibility)
     * @return Number of decimal places
     */
    int32_t getPrecision() const { return MAX_PRECISION; }
    
    /**
     * @brief Set precision (no-op for compatibility)
     * @param precision New precision value
     */
    void setPrecision(int32_t precision);
    
    /**
     * @brief Check if value is zero
     * @return True if zero
     */
    bool isZero() const;
    
    /**
     * @brief Check if value is positive
     * @return True if positive
     */
    bool isPositive() const;
    
    /**
     * @brief Check if value is negative
     * @return True if negative
     */
    bool isNegative() const;
    
    /**
     * @brief Get absolute value
     * @return Absolute value
     */
    Decimal abs() const;
    
    /**
     * @brief Round to specified precision
     * @param precision Target precision
     * @return Rounded decimal
     */
    Decimal round(int32_t precision) const;
    
    /**
     * @brief Square root
     * @return Square root
     */
    Decimal sqrt() const;
    
    /**
     * @brief Power
     * @param exponent Exponent
     * @return Result of power operation
     */
    Decimal pow(int32_t exponent) const;
    
    // Static factory methods
    static Decimal fromString(const std::string& str, int32_t precision = DEFAULT_PRECISION);
    static Decimal fromDouble(double val, int32_t precision = DEFAULT_PRECISION);
    static Decimal fromInt(int64_t val, int32_t precision = DEFAULT_PRECISION);
    
    // Constants
    static Decimal ZERO;
    static Decimal ONE;
    static Decimal TEN;
    static Decimal HUNDRED;
    
public:
    /**
     * @brief Internal constructor for Boost decimal values
     * @param val Boost decimal value
     */
    Decimal(const boost_decimal& val) : value_(val) {}
    
private:
    /**
     * @brief Get the underlying Boost decimal value
     * @return Boost decimal value
     */
    const boost_decimal& getBoostDecimal() const { return value_; }
};

// Stream operators
std::ostream& operator<<(std::ostream& os, const Decimal& decimal);
std::istream& operator>>(std::istream& is, Decimal& decimal);

} // namespace utils
} // namespace okx
