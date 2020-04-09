import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input',
                            help='Input for the pipeline',
                            default='./data/sp500.csv')
        parser.add_argument('--output',
                            help='Output for the pipeline',
                            default='./outputs/result.txt')


class Split(beam.DoFn):
    def process(self, element):
        date, open, high, low, close, volume = element.split(",")
        return [{
            'Open': float(open),
            'Close': float(close),
        }]


class CollectOpen(beam.DoFn):
    def process(self, element):
        # Returns a list of tuples containing Date and Open value
        return [(1, element['Open'])]


class CollectClose(beam.DoFn):
    def process(self, element, *args, **kwargs):
        # Returns a list of tuples containing the 1 key and Close value
        return [(1, element['Close'])]


# create the pipeline
options = MyOptions()

p = beam.Pipeline(options=options)

# split the input from csv file
csv_lines = (
    p | beam.io.ReadFromText(p.options.input, skip_header_lines=1) |
    beam.ParDo(Split())
)

# calculate the mean for open values
mean_open = (
    csv_lines | beam.ParDo(CollectOpen()) |
    "Grouping Keys Open" >> beam.GroupByKey() |
    "Calculating Open Mean" >> beam.CombineValues(
        beam.combiners.MeanCombineFn()
    )
)

# calculate the mean for closed values
mean_close = (
    csv_lines | beam.ParDo(CollectClose()) |
    "Grouping Keys Close" >> beam.GroupByKey() |
    "Calculating Close Mean" >> beam.CombineValues(
        beam.combiners.MeanCombineFn()
    )
)

# writing results to file
output = (
    {
        'Mean Open': mean_open,
        'Mean Close': mean_close
    } |
    beam.CoGroupByKey() |
    beam.io.WriteToText(p.options.output)
)

result = p.run()
result.wait_until_finish()
