import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from feature_pipeline.entities import Flight, flight_avro_schema, airport_avro_schema


def parse_csv(line: str):
    import csv
    return next(csv.reader([line]))


def parse_line(fields):
    from datetime import datetime
    from apache_beam.utils.timestamp import Timestamp
    from feature_pipeline.entities import Flight
    from feature_pipeline.helpers import csv_headers

    data = dict(zip(csv_headers, fields))

    if (
        data["Year"] != "Year"  # skip header row
        and len(data["WheelsOff"]) == 4  #
        and len(data["FlightDate"]) == 10  # row has a flight date
        and data["Distance"] != ""
    ):
        wheels_off_hour = data["WheelsOff"][:2]
        wheels_off_minutes = data["WheelsOff"][2:]
        departure_date_time = (
            f"{data['FlightDate']}T{wheels_off_hour}:{wheels_off_minutes}:00"
        )

        cancelled = (float(data["Cancelled"]) > 0) or (float(data["Diverted"]) > 0)

        try:
            flight = Flight(
                timestamp=datetime.fromisoformat(departure_date_time),
                origin_airport_id=str(data["OriginAirportID"]),
                flight_number=f"{data['Reporting_Airline']}//{data['Flight_Number_Reporting_Airline']}",
                is_cancelled=cancelled,
                departure_delay_minutes=float(data["DepDelay"]),
                arrival_delay_minutes=float(data["ArrDelay"]),
                taxi_out_minutes=float(data["TaxiOut"]),
                distance_miles=float(data["Distance"]),
            )

            yield beam.window.TimestampedValue(
                flight, Timestamp.from_rfc3339(departure_date_time)
            )
        except:
            pass


class BuildTimestampedRecordFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        from feature_pipeline.entities import AirportFeatures

        window_start = window.start.to_utc_datetime()
        return [
            AirportFeatures(
                timestamp=window_start,
                origin_airport_id=element.origin_airport_id,
                average_departure_delay=element.average_departure_delay,
            )._asdict()
        ]


class BuildTimestampedFlightRecordFn(beam.DoFn):
    def process(self, element: Flight, window=beam.DoFn.WindowParam):
        return [element._asdict()]


def run(argv=None, save_main_session=False):
    """Main entry point; defines and runs the wordcount pipeline."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        dest="input",
        default="/Users/simon/projects/private/gcp_mlops/data/processed/2019/2019-01.csv",
        help="Input file to process.",
    )
    parser.add_argument(
        "--output-airports",
        dest="output_airports",
        default="/Users/simon/projects/private/gcp_mlops/data/output_airports/",
        help="Output file to write results to.",
    )

    parser.add_argument(
        "--output-flights",
        dest="output_flights",
        default="/Users/simon/projects/private/gcp_mlops/data/output_flights/",
        help="Output file to write results to.",
    )

    parser.add_argument(
        "--output-read-instances",
        dest="output_read_instances",
        default="/Users/simon/projects/private/gcp_mlops/data/output_read_instances/",
        help="Output file to write results to.",
    )

    # Parse beam arguments (e.g. --runner=DirectRunner to run the pipeline locally)
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as pipeline:
        flights = (
            pipeline
            | "read_input" >> beam.io.ReadFromText(known_args.input)
            | "parse_csv" >> beam.Map(parse_csv)
            | "create_flight_obj" >> beam.FlatMap(parse_line).with_output_types(Flight)
        )

        # Create airport data
        (
            flights
            | "window"
            >> beam.WindowInto(
                beam.window.SlidingWindows(4 * 60 * 60, 60 * 60)
            )  # 4h time windows, every 60min
            | "group_by_airport"
            >> beam.GroupBy("origin_airport_id").aggregate_field(
                "departure_delay_minutes",
                beam.combiners.MeanCombineFn(),
                "average_departure_delay",
            )
            | "add_timestamp" >> beam.ParDo(BuildTimestampedRecordFn())
            | "write_airport_data"
            >> beam.io.WriteToAvro(
                known_args.output_airports, schema=airport_avro_schema
            )
        )

        # Create flight data
        (
            flights
            | "format_output" >> beam.ParDo(BuildTimestampedFlightRecordFn())
            | "write_flight_data"
            >> beam.io.WriteToAvro(known_args.output_flights, schema=flight_avro_schema)
        )

        # Create read_instances.csv to retrieve training data from the feature store
        (
            flights
            | "format_read_instances_output"
            >> beam.Map(
                lambda flight: f"{flight.flight_number},{flight.origin_airport_id},{flight.timestamp.isoformat('T') + 'Z'}"
            )
            | "write_read_instances"
            >> beam.io.WriteToText(
                known_args.output_read_instances,
                file_name_suffix=".csv",
                num_shards=1,
                header="flight,airport,timestamp",
            )
        )
