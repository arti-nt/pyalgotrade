# PyAlgoTrade
#
# Copyright 2011-2015 Gabriel Martin Becedillas Ruiz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
.. moduleauthor:: Melnikov Artyom <a@arti-nt.ru>
"""

from pyalgotrade.barfeed import membf, common
from pyalgotrade.utils import dt
from pyalgotrade import bar

import datetime
import struct


######################################################################
## Dukascopy ticks parser
#
# Ticks Format:
# 4 bytes >i time (seconds from midnight)
# 4 bytes >i ask * exp (for EURUSD exp == 5)
# 4 bytes >i bid * exp
# 8 bytes unknown
#
# Originally, ticks are compressed with lzma, must be unpacked before parsing


class Feed(membf.BarFeed):
    """A :class:`pyalgotrade.barfeed.csvfeed.BarFeed` that loads bars from CSV files downloaded from Yahoo! Finance.

    :param frequency: The frequency of the bars.
    :param timezone: The default timezone to use to localize bars.
    :type timezone: A pytz timezone.
    :param maxLen: The maximum number of values that the :class:`pyalgotrade.dataseries.bards.BarDataSeries` will hold.
        Once a bounded length is full, when new items are added, a corresponding number of items are discarded from the
        opposite end. If None then dataseries.DEFAULT_MAX_LEN is used.
    :type maxLen: int.

    .. note::
        Yahoo! Finance csv files lack timezone information.
        When working with multiple instruments:

            * If all the instruments loaded are in the same timezone, then the timezone parameter may not be specified.
            * If any of the instruments loaded are in different timezones, then the timezone parameter must be set.
    """

    def __init__(self, frequency=bar.Frequency.DAY, timezone=None, maxLen=None):
        if timezone is not None:
            raise Exception("timezone is not supported, sorry.")

        if frequency not in [bar.Frequency.TRADE,
                             bar.Frequency.SECOND,
                             bar.Frequency.MINUTE,
                             bar.Frequency.FIVE_MINUTES,
                             bar.Frequency.FIFTEEN_MINUTES,
                             bar.Frequency.THIRTY_MINUTES,
                             bar.Frequency.HOUR,
                             bar.Frequency.FOUR_HOURS,
                             bar.Frequency.DAY]:
            raise Exception("Invalid frequency.")

        super(Feed, self).__init__(frequency, maxLen)

        self.__timezone = timezone
        self.__sanitizeBars = False
        self.__barClass = bar.BasicBar

    def setBarClass(self, barClass):
        self.__barClass = barClass

    def sanitizeBars(self, sanitize):
        self.__sanitizeBars = sanitize

    def barsHaveAdjClose(self):
        return True

    def loadTicksFromFile(self, path):
        ticks = []
        with open(path, "rb") as f:
            while True:
                tick_bytes = f.read(20)
                if len(tick_bytes) == 0:
                    break
                time_msec, ask, bid = struct.unpack_from('>iii', tick_bytes)
                ### TODO: make reading independent on currency pair
                ticks.append({'ask': ask / 100000.0, 'bid': bid / 100000.0, 'time': time_msec})

        return ticks

    def addBarsFromFile(self, instrument, date, path, frequency, timezone=None):
        """Loads ticks and generates bars for a given instrument from a raw dukascopy ticks file.
        The instrument gets registered in the bar feed.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param date: Day.
        :type date: string, %Y.%m.%d format.
        :param path: The path to the ticks file.
        :type path: string.
        :param timezone: The timezone to use to localize bars. Check :mod:`pyalgotrade.marketsession`.
        :type timezone: A pytz timezone.
        """

        if timezone is not None:
            raise Exception('timezone is not supported, sorry.')

        ticks = self.loadTicksFromFile(path)
        bars = []

        if frequency == bar.Frequency.TRADE:
            for tick in ticks:
                bars.append(self.__barClass(
                    datetime.datetime.strptime(date, '%Y.%m.%d') + \
                        datetime.timedelta(milliseconds = tick['time']),
                    tick['bid'],
                    tick['bid'],
                    tick['bid'],
                    tick['bid'],
                    10000,
                    None,
                    frequency))
        else:
            lastBarNum = None
            lastBar = {}

            # convert frequency to msecs
            frequency *= 1000

            for tick in ticks:
                if lastBarNum is None or tick['time'] / frequency != lastBarNum:
                    if lastBarNum is not None:
                        bars.append(self.__barClass(
                            lastBar['dateTime'],
                            lastBar['open_'],
                            lastBar['high'],
                            lastBar['low'],
                            lastBar['curr'], # close price
                            lastBar['volume'],
                            lastBar['adjClose'],
                            frequency))

                    lastBar['dateTime'] = datetime.datetime.strptime(date, '%Y.%m.%d') + \
                                          datetime.timedelta(milliseconds = tick['time'] - tick['time'] % frequency)
                    lastBar['open_'] = tick['bid'] if lastBarNum is None else lastBar['curr']
                    lastBar['high'] = lastBar['open_']
                    lastBar['low'] = lastBar['open_']
                    lastBar['curr'] = lastBar['open_']
                    lastBar['volume'] = 10000
                    lastBar['adjClose'] = None
                    lastBarNum = tick['time'] / frequency

                lastBar['curr'] = tick['bid']
                lastBar['low'] = min(lastBar['low'], lastBar['curr'])
                lastBar['high'] = max(lastBar['high'], lastBar['curr'])

        super(Feed, self).addBarsFromSequence(instrument, bars)
