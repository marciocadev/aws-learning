from aws_cdk import (
    core,
    aws_dynamodb as _dynamo,
    aws_lambda as _lambda,
    aws_apigateway as _gateway,
    aws_sqs as _sqs,
    aws_secretsmanager as _secrets,
    aws_lambda_event_sources as _event
)


class StockHistoryStack(core.Stack):

    def __init__(self, scope: core.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        secret = _secrets.Secret.from_secret_name(
            scope=self,
            id="secret",
            secret_name="yahoo-finance"
        )

        # The code that defines your stack goes here
        table_name_price = "stock-price-v1"
        price_table = _dynamo.Table(
            scope=self,
            id="StockPriceDynamo",
            table_name=table_name_price,
            removal_policy=core.RemovalPolicy.DESTROY,
            billing_mode=_dynamo.BillingMode.PAY_PER_REQUEST,
            partition_key=_dynamo.Attribute(
                name="Hash",
                type=_dynamo.AttributeType.STRING
            )
        )
        price_table.add_global_secondary_index(
            partition_key=_dynamo.Attribute(
                name="hash_key",
                type=_dynamo.AttributeType.STRING
            ),
            sort_key=_dynamo.Attribute(
                name="Date",
                type=_dynamo.AttributeType.STRING
            ),
            index_name="dt_price_index"
        )

        table_name_dividend = "stock-dividend-v1"
        dividend_table = _dynamo.Table(
            scope=self,
            id="StockDividendDynamo",
            table_name=table_name_dividend,
            removal_policy=core.RemovalPolicy.DESTROY,
            billing_mode=_dynamo.BillingMode.PAY_PER_REQUEST,
            partition_key=_dynamo.Attribute(
                name="Hash",
                type=_dynamo.AttributeType.STRING
            )
        )
        price_table.add_global_secondary_index(
            partition_key=_dynamo.Attribute(
                name="hash_key",
                type=_dynamo.AttributeType.STRING
            ),
            sort_key=_dynamo.Attribute(
                name="Date",
                type=_dynamo.AttributeType.STRING
            ),
            index_name="dt_dividend_index"
        )

        table_name_split = "stock-split-v1"
        split_table = _dynamo.Table(
            scope=self,
            id="StockSplitDynamo",
            table_name=table_name_split,
            removal_policy=core.RemovalPolicy.DESTROY,
            billing_mode=_dynamo.BillingMode.PAY_PER_REQUEST,
            partition_key=_dynamo.Attribute(
                name="Hash",
                type=_dynamo.AttributeType.STRING
            )
        )
        split_table.add_global_secondary_index(
            partition_key=_dynamo.Attribute(
                name="hash_key",
                type=_dynamo.AttributeType.STRING
            ),
            sort_key=_dynamo.Attribute(
                name="Date",
                type=_dynamo.AttributeType.STRING
            ),
            index_name="dt_split_index"
        )

        sqs_dlq_queue_stock = _sqs.Queue(
            scope=self,
            id="StockSQSDLQ",
            queue_name="sqs-dql-stock-v1",
            retention_period=core.Duration.minutes(15),
            visibility_timeout=core.Duration.seconds(300)
        )
        sqs_dlq_stock = _sqs.DeadLetterQueue(
            max_receive_count=500,
            queue=sqs_dlq_queue_stock
        )

        sqs_stock = _sqs.Queue(
            scope=self,
            id="StockSQS",
            queue_name="sqs-stock-v1",
            retention_period=core.Duration.minutes(15),
            dead_letter_queue=sqs_dlq_stock
        )

        lambda_dynamo_sh_name = "persist-daily-stock-price-v1"
        lambda_dynamo_sh =_lambda.Function(
            scope=self,
            id="StockHistoryLambdaPersist",
            function_name=lambda_dynamo_sh_name,
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler="app.lambda_handler",
            code=_lambda.Code.from_asset("stock/persist"),
            timeout=core.Duration.seconds(30),
            environment={
                "SQS_URL": sqs_stock.queue_url,
                "TABLE_PRICE_NAME": table_name_price,
                "TABLE_DIVIDEND_NAME": table_name_dividend,
                "TABLE_SPLIT_NAME": table_name_split, 
                "SECRET": "yahoo-finance",
                "REGION": "us-east-1"
            }
        )

        price_table.grant_write_data(lambda_dynamo_sh)
        dividend_table.grant_write_data(lambda_dynamo_sh)
        split_table.grant_write_data(lambda_dynamo_sh)
        secret.grant_read(lambda_dynamo_sh)
        sqs_stock.grant_consume_messages(lambda_dynamo_sh)
        sqs_stock.grant_send_messages(lambda_dynamo_sh)
        lambda_dynamo_sh.add_event_source(_event.SqsEventSource(sqs_stock))

        lambda_gateway_sh_name = 'gateway-daily-stock-price-v1'
        lambda_gateway_sh = _lambda.Function(
            scope=self,
            id="StockHistoryLambdaPost",
            function_name=lambda_gateway_sh_name,
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler="app.lambda_handler",
            code=_lambda.Code.from_asset("stock/post"),
            timeout=core.Duration.seconds(30),
            environment={
                "SQS_URL": sqs_stock.queue_url
            }
        )

        sqs_stock.grant_send_messages(lambda_gateway_sh)

        integration_lambda_post_sh = _gateway.LambdaIntegration(
            handler=lambda_gateway_sh
        )

        gateway_rest_name = "stock-gateway-v1"
        gateway_rest = _gateway.RestApi(
            scope=self,
            id="GatewayStock",
            rest_api_name=gateway_rest_name,
            default_cors_preflight_options=_gateway.CorsOptions(
                allow_origins=_gateway.Cors.ALL_ORIGINS,
                allow_methods=_gateway.Cors.ALL_METHODS
            )
        )

        gateway_stock_post_validator_name = "persist-daily-stok-price-post-validation"
        gateway_stock_post_validator = _gateway.RequestValidator(
            scope=self,
            id="PostValidator",
            request_validator_name=gateway_stock_post_validator_name,
            rest_api=gateway_rest,
            validate_request_body=False,
            validate_request_parameters=True
        )

        root = gateway_rest.root
        resource = root.add_resource("v1")

        resource_name = "persist-stock-post-v1"
        resource.add_method(
            http_method="POST",
            integration=integration_lambda_post_sh,
            operation_name=resource_name,
            request_parameters={
                "method.request.querystring.stock": True
            },
            request_validator=gateway_stock_post_validator
        )