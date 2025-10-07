#pragma once

#include <string>
#include <cstdint>
#include <stdexcept>
#include <iostream>

namespace okx {
namespace utils {

/**
 * @brief High-precision decimal class for financial calculations
 * 
 * This class provides decimal arithmetic similar to Python's Decimal class,
 * avoiding floating-point precision issues in financial calculations.
 */
class Decimal {
private:
    int64_t value_;      // Stored as integer (scaled by precision)
    int32_t precision_;  // Number of decimal places
    
    static constexpr int32_t DEFAULT_PRECISION = 16;
    static constexpr int32_t MAX_PRECISION = 36;
    
public:
    /**
     * @brief Default constructor (creates 0.0)
     */
    Decimal() : value_(0), precision_(DEFAULT_PRECISION) {}
    
    /**
     * @brief Constructor from double
     * @param val Double value to convert
     * @param precision Number of decimal places (default: 16)
     */
    explicit Decimal(double val, int32_t precision = DEFAULT_PRECISION);
    
    /**
     * @brief Constructor from string
     * @param val String representation of decimal number
     * @param precision Number of decimal places (default: 16)
     */
    explicit Decimal(const std::string& val, int32_t precision = DEFAULT_PRECISION);
    
    /**
     * @brief Constructor from integer
     * @param val Integer value
     * @param precision Number of decimal places (default: 16
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
     * @brief Get precision
     * @return Number of decimal places
     */
    int32_t getPrecision() const { return precision_; }
    
    /**
     * @brief Set precision
     * @param precision New precision value
     */
    void setPrecision(int32_t precision);
    
    /**
     * @brief Check if value is zero
     * @return True if zero
     */
    bool isZero() const { return value_ == 0; }
    
    /**
     * @brief Check if value is positive
     * @return True if positive
     */
    bool isPositive() const { return value_ > 0; }
    
    /**
     * @brief Check if value is negative
     * @return True if negative
     */
    bool isNegative() const { return value_ < 0; }
    
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
    
private:
    /**
     * @brief Internal constructor for already scaled values
     * @param val Already scaled value
     * @param precision Precision
     */
    Decimal(int64_t val, int32_t precision, bool /*internal*/) : value_(val), precision_(precision) {}
    
    /**
     * @brief Normalize precision between two decimals
     * @param other Other decimal
     * @return Pair of normalized values
     */
    std::pair<int64_t, int64_t> normalizePrecision(const Decimal& other) const;
    
    /**
     * @brief Calculate scale factor for precision
     * @param precision Target precision
     * @return Scale factor
     */
    static int64_t getScaleFactor(int32_t precision);
    
    /**
     * @brief Parse string to decimal
     * @param str String to parse
     * @return Parsed value
     */
    static int64_t parseString(const std::string& str, int32_t precision);
};

// Stream operators
std::ostream& operator<<(std::ostream& os, const Decimal& decimal);
std::istream& operator>>(std::istream& is, Decimal& decimal);

} // namespace utils
} // namespace okx
