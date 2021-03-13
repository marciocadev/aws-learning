#!/usr/bin/env python3

from aws_cdk import core

from stock_history.stock_history_stack import StockHistoryStack


app = core.App()
StockHistoryStack(app, "stock-history")

app.synth()
