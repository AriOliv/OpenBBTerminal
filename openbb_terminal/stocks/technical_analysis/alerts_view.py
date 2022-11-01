""" Alerts View """
__docformat__ = "numpy"

import logging
import os

from openbb_terminal.decorators import log_start_end
from openbb_terminal.helper_funcs import export_data, print_rich_table
from openbb_terminal.rich_config import console
from openbb_terminal.stocks.technical_analysis import alerts_model

logger = logging.getLogger(__name__)


@log_start_end(log=logger)
def display_alerts():
    df = alerts_model.get_alerts()
    print_rich_table(df, headers=list(df.columns), show_index=False, title="ALERTS!")