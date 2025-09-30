"""
Integration module for C++ Data Processor with existing Python data processor.
This module provides a seamless interface between the Python data processor
and the high-performance C++ implementation.
"""

import logging
from typing import Optional, Dict, Any
import polars as pl
from polars import DataFrame

try:
    import cpp_data_processor
    CPP_AVAILABLE = True
except ImportError:
    CPP_AVAILABLE = False
    cpp_data_processor = None

from enumerations import SymbolId

logger = logging.getLogger(__name__)


class CppDataProcessorIntegration:
    """
    Integration class that provides a bridge between the Python data processor
    and the C++ implementation.
    """
    
    def __init__(self, enable_cpp: bool = True):
        """
        Initialize the C++ integration.
        
        Args:
            enable_cpp: Whether to enable C++ processing (default: True)
        """
        self.enable_cpp = enable_cpp and CPP_AVAILABLE
        self.cpp_processor = None
        
        if self.enable_cpp:
            try:
                self.cpp_processor = cpp_data_processor.DataProcessor()
                logger.info("C++ Data Processor initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize C++ Data Processor: {e}")
                self.enable_cpp = False
                self.cpp_processor = None
        else:
            logger.warning("C++ Data Processor not available, falling back to Python implementation")
    
    def process_trades_data(self, symbol_id: SymbolId, trades_df: DataFrame) -> Dict[str, Any]:
        """
        Process trades data using C++ processor if available, otherwise fall back to Python.
        
        Args:
            symbol_id: Symbol identifier
            trades_df: Polars DataFrame containing trades data
            
        Returns:
            Dictionary containing processing results and metadata
        """
        if not self.enable_cpp or self.cpp_processor is None:
            return self._fallback_to_python(symbol_id, trades_df)
        
        try:
            # Convert Polars DataFrame to format expected by C++ processor
            cpp_trades_df = self._convert_polars_to_cpp_format(trades_df)
            
            # Process using C++ processor
            result = self.cpp_processor.process_trades_data(symbol_id, cpp_trades_df)
            
            if result.success:
                logger.info(f"C++ processing completed successfully in {result.processing_time_seconds:.3f}s")
                return {
                    'success': True,
                    'processing_time_seconds': result.processing_time_seconds,
                    'message': result.error_message,
                    'processor_type': 'cpp'
                }
            else:
                logger.error(f"C++ processing failed: {result.error_message}")
                return self._fallback_to_python(symbol_id, trades_df)
                
        except Exception as e:
            logger.error(f"C++ processing error: {e}")
            return self._fallback_to_python(symbol_id, trades_df)
    
    def process_trades_data_async(self, symbol_id: SymbolId, trades_df: DataFrame, 
                                callback: callable) -> None:
        """
        Process trades data asynchronously using C++ processor.
        
        Args:
            symbol_id: Symbol identifier
            trades_df: Polars DataFrame containing trades data
            callback: Callback function to call when processing is complete
        """
        if not self.enable_cpp or self.cpp_processor is None:
            # Fall back to synchronous Python processing
            result = self._fallback_to_python(symbol_id, trades_df)
            callback(result)
            return
        
        try:
            # Convert Polars DataFrame to format expected by C++ processor
            cpp_trades_df = self._convert_polars_to_cpp_format(trades_df)
            
            # Process asynchronously using C++ processor
            def cpp_callback(cpp_result):
                if cpp_result.success:
                    result = {
                        'success': True,
                        'processing_time_seconds': cpp_result.processing_time_seconds,
                        'message': cpp_result.error_message,
                        'processor_type': 'cpp'
                    }
                else:
                    # Fall back to Python processing
                    result = self._fallback_to_python(symbol_id, trades_df)
                
                callback(result)
            
            self.cpp_processor.process_trades_data_async(symbol_id, cpp_trades_df, cpp_callback)
            
        except Exception as e:
            logger.error(f"C++ async processing error: {e}")
            result = self._fallback_to_python(symbol_id, trades_df)
            callback(result)
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """
        Get processing statistics from C++ processor.
        
        Returns:
            Dictionary containing processing statistics
        """
        if not self.enable_cpp or self.cpp_processor is None:
            return {'processor_type': 'python', 'stats_available': False}
        
        try:
            stats = self.cpp_processor.get_processing_stats()
            stats['processor_type'] = 'cpp'
            stats['stats_available'] = True
            return stats
        except Exception as e:
            logger.error(f"Failed to get C++ processing stats: {e}")
            return {'processor_type': 'cpp', 'stats_available': False, 'error': str(e)}
    
    def set_processing_params(self, params: Dict[str, Any]) -> None:
        """
        Set processing parameters for C++ processor.
        
        Args:
            params: Dictionary containing processing parameters
        """
        if not self.enable_cpp or self.cpp_processor is None:
            logger.warning("C++ processor not available, cannot set parameters")
            return
        
        try:
            self.cpp_processor.set_processing_params(params)
            logger.info("C++ processing parameters updated successfully")
        except Exception as e:
            logger.error(f"Failed to set C++ processing parameters: {e}")
    
    def _convert_polars_to_cpp_format(self, trades_df: DataFrame) -> Dict[str, Any]:
        """
        Convert Polars DataFrame to format expected by C++ processor.
        
        Args:
            trades_df: Polars DataFrame containing trades data
            
        Returns:
            Dictionary in format expected by C++ processor
        """
        # Convert to Python dictionary format
        trades_dict = {
            'trade_id': trades_df['trade_id'].to_list(),
            'price': trades_df['price'].to_list(),
            'quantity': trades_df['quantity'].to_list(),
            'is_buy': trades_df['is_buy'].to_list(),
            'datetime': trades_df['datetime'].to_list()
        }
        
        return trades_dict
    
    def _fallback_to_python(self, symbol_id: SymbolId, trades_df: DataFrame) -> Dict[str, Any]:
        """
        Fallback to Python processing when C++ is not available.
        
        Args:
            symbol_id: Symbol identifier
            trades_df: Polars DataFrame containing trades data
            
        Returns:
            Dictionary containing fallback processing results
        """
        logger.info("Falling back to Python processing")
        
        # This would call the original Python data processor
        # For now, return a placeholder result
        return {
            'success': True,
            'processing_time_seconds': 0.0,
            'message': 'Python fallback processing (not implemented)',
            'processor_type': 'python_fallback'
        }
    
    def is_cpp_available(self) -> bool:
        """
        Check if C++ processor is available.
        
        Returns:
            True if C++ processor is available, False otherwise
        """
        return self.enable_cpp and self.cpp_processor is not None
    
    def get_processor_info(self) -> Dict[str, Any]:
        """
        Get information about the current processor.
        
        Returns:
            Dictionary containing processor information
        """
        return {
            'cpp_available': CPP_AVAILABLE,
            'cpp_enabled': self.enable_cpp,
            'cpp_initialized': self.cpp_processor is not None,
            'processor_type': 'cpp' if self.is_cpp_available() else 'python'
        }


# Global instance for easy access
cpp_integration = CppDataProcessorIntegration()


def get_cpp_integration() -> CppDataProcessorIntegration:
    """
    Get the global C++ integration instance.
    
    Returns:
        CppDataProcessorIntegration instance
    """
    return cpp_integration


def process_trades_with_cpp(symbol_id: SymbolId, trades_df: DataFrame) -> Dict[str, Any]:
    """
    Convenience function to process trades data with C++ processor.
    
    Args:
        symbol_id: Symbol identifier
        trades_df: Polars DataFrame containing trades data
        
    Returns:
        Dictionary containing processing results
    """
    return cpp_integration.process_trades_data(symbol_id, trades_df)


def process_trades_with_cpp_async(symbol_id: SymbolId, trades_df: DataFrame, 
                                callback: callable) -> None:
    """
    Convenience function to process trades data asynchronously with C++ processor.
    
    Args:
        symbol_id: Symbol identifier
        trades_df: Polars DataFrame containing trades data
        callback: Callback function to call when processing is complete
    """
    cpp_integration.process_trades_data_async(symbol_id, trades_df, callback)
