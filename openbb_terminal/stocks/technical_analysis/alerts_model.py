""" Alerts Model """
__docformat__ = "numpy"
import logging
import asyncio

import pandas as pd

from alerts import alert_bot

from openbb_terminal.decorators import log_start_end
from openbb_terminal.helper_funcs import get_user_agent

logger = logging.getLogger(__name__)

@log_start_end(log=logger)
def get_alerts() -> pd.DataFrame:

    df = asyncio.run(alert_bot.main())
    return df