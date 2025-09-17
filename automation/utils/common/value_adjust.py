#coding: utf-8
"""Adjustment Value Utilities

"""

import logging
import pandas as pd
import numpy as np


logger = logging.getLogger("automation.utils.common.value_adjust")

class AdjustValue:
    """Adjust Value
    
    
    There are several methods to adjust value:
    1. Setup value by fixed value
    2. Add fixed value to value
    3. Offset value by range
    4. Setup value by ladder configuration
    """
    def __init__(self):
        pass
    
    @classmethod
    def setup_value_by_fixed_value(cls, fixed_value: float) -> float:
        """Setup Value by Fixed Value
        
        Args:
        ------------
            fixed_value (float): Fixed value to setup the value
        
        Returns:
        ------------
            float: Adjusted value
        """
        
        try:
            fixed_value = cls._check_value_validate(fixed_value)
        except ValueError as e:
            logger.error(f"Error validating commission rates: {e}")
            raise ValueError(f"Error validating commission rates by setup fixed commission rate: {e}")
        return fixed_value

    @classmethod
    def add_with_fixed_value(cls, value: float, fixed_value: float) -> float:
        """Add Fixed Value to Value
        
        Args:
        ------------
            value (float): Original value
            fixed_value (float): Fixed value to add to the value
        Returns:
        ------------
            float: Adjusted value
        """
        value += fixed_value
        try:
            value = cls._check_value_validate(value)
        except ValueError as e:
            logger.error(f"Error validating value: {e}")
            raise ValueError(f"Error validating value by adding fixed value: {e}")
        return value
        


    @classmethod
    def offset_range_value(cls, value: float, low_threshold: float, high_threshold: float, keep_over_high_threshold: bool = False) -> float:
        """Offset Value by Range
       
        Example:
        ------------
        >>> AdjustValue.offset_range_value(0.056, 0.07, 0.086)
            0.0804186046511628
        >>> AdjustValue.offset_range_value(0.092, 0.07, 0.086)
            0.08711627906976743
        >>> AdjustValue.offset_range_value(0.092, 0.07, 0.086, keep_over_high_threshold=True)
            0.092
            
        Args:
        ------------
            value (float): Value to be adjusted
            low_threshold (float): Lower threshold
            high_threshold (float): Upper threshold
            keep_over_high_threshold (bool): Whether to keep values over the high threshold unchanged
        
        Returns:
        ------------
            float: Adjusted value
        """
        
        if keep_over_high_threshold and value >= high_threshold:
            adjust_value = value
        else:
            adjust_value = high_threshold + (high_threshold - low_threshold)  / high_threshold * (value - high_threshold)

        return cls._check_value_validate(adjust_value)


    @classmethod
    def setup_by_ladder_config(cls, condition_value: float, ladder_condition_config: list[float], ladder_value_config: list[float]) -> float:
        """Setup Value by Ladder Configuration
        
        Use case condition length equal with value length: ladder_condition_config = [0.3, 0.5, 0.7], ladder_value_config = [0.1, 0.2, 0.3]
            if condition_value <= 0.3, then adjusted_value = 0.1
            if 0.3 < condition_value <= 0.5, then adjusted_value = 0.2
            if 0.5 < condition_value <= 0.7, then adjusted_value = 0.3
            if condition_value > 0.7, then adjusted_value = 0.3 (the last value in ladder_value_config)
        Use case condition length 1 less than value length: ladder_condition_config = [0.3, 0.5, 0.7], ladder_value_config = [0.1, 0.2, 0.3, 0.4]
            if condition_value <= 0.3, then adjusted_value = 0.1
            if 0.3 < condition_value <= 0.5, then adjusted_value = 0.2
            if 0.5 < condition_value <= 0.7, then adjusted_value = 0.3
            if condition_value > 0.7, then adjusted_value = 0.4 (the last value in ladder_value_config)
        
        Args:
        ------------
            condition_value (float): Value to be adjusted
            ladder_condition_config (list[float]): Ladder condition configuration
            ladder_value_config (list[float]): Ladder value configuration
        
        Returns:
        ------------
            float: Adjusted commission rate
        """
        if len(ladder_value_config) - len(ladder_condition_config) == 1:
            hat_value = ladder_value_config[-1]
            ladder_condition_config.append(np.inf)
        else:
            hat_value = ladder_value_config[-1]
        
        if len(ladder_condition_config) != len(ladder_value_config):
            raise ValueError("Ladder condition and value configurations must have the same length.")
        
        adjusted_value = -1.0
        # Find the appropriate ladder condition and return the corresponding ladder value
        for i in range(len(ladder_condition_config)):
            if condition_value <= ladder_condition_config[i]:
                try:
                    adjusted_value = ladder_value_config[i]
                except ValueError as e:
                    logger.error(f"Error validating commission rates: {e}")
                    raise ValueError(f"Error validating commission rates by setup ladder commission rate: {e}")
                break


        if adjusted_value == -1.0:
            adjusted_value = hat_value

        return cls._check_value_validate(adjusted_value)
        

    @staticmethod
    def _check_value_validate(value: float, min_value: float=0.0, max_value: float=1.0) -> float:
        """Check Value Validity
        
        Args:
        ------------
            value (float): Value to be validated
            min_value (float): Minimum valid value
            max_value (float): Maximum valid value

        Returns:
        ------------
            float: Validated value
        """
        if pd.isna(value):
            raise ValueError("Value contains null values.")

        if value < min_value or value > max_value:
            raise ValueError(f"Value must be between {min_value} and {max_value}.")

        return value


