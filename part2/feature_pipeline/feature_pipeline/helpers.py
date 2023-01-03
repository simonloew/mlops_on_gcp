from typing import Union, get_args, get_origin
from datetime import datetime


def map_to_avro_type(field_type):
    if field_type == str:
        return "string"
    elif field_type == bool:
        return "boolean"
    elif field_type == float:
        return "double"
    elif field_type is type(None):
        return "null"
    elif field_type == datetime:
        return {"type": "long", "logicalType": "timestamp-micros"}
    elif get_origin(field_type) == Union:
        return [map_to_avro_type(t) for t in get_args(field_type)]
    else:
        raise NotImplementedError(f"Unsupported type: {field_type}")


def named_tuple_to_avro_fields(named_tuple):
    fields = []
    for field_name, field_type in named_tuple.__annotations__.items():
        fields.append({"name": field_name, "type": map_to_avro_type(field_type)})
    return fields


csv_headers = [
    "Year",
    "Quarter",
    "Month",
    "DayofMonth",
    "DayOfWeek",
    "FlightDate",
    "Reporting_Airline",
    "DOT_ID_Reporting_Airline",
    "IATA_CODE_Reporting_Airline",
    "Tail_Number",
    "Flight_Number_Reporting_Airline",
    "OriginAirportID",
    "OriginAirportSeqID",
    "OriginCityMarketID",
    "Origin",
    "OriginCityName",
    "OriginState",
    "OriginStateFips",
    "OriginStateName",
    "OriginWac",
    "DestAirportID",
    "DestAirportSeqID",
    "DestCityMarketID",
    "Dest",
    "DestCityName",
    "DestState",
    "DestStateFips",
    "DestStateName",
    "DestWac",
    "CRSDepTime",
    "DepTime",
    "DepDelay",
    "DepDelayMinutes",
    "DepDel15",
    "DepartureDelayGroups",
    "DepTimeBlk",
    "TaxiOut",
    "WheelsOff",
    "WheelsOn",
    "TaxiIn",
    "CRSArrTime",
    "ArrTime",
    "ArrDelay",
    "ArrDelayMinutes",
    "ArrDel15",
    "ArrivalDelayGroups",
    "ArrTimeBlk",
    "Cancelled",
    "CancellationCode",
    "Diverted",
    "CRSElapsedTime",
    "ActualElapsedTime",
    "AirTime",
    "Flights",
    "Distance",
    "DistanceGroup",
    "CarrierDelay",
    "WeatherDelay",
    "NASDelay",
    "SecurityDelay",
    "LateAircraftDelay",
    "FirstDepTime",
    "TotalAddGTime",
    "LongestAddGTime",
    "DivAirportLandings",
    "DivReachedDest",
    "DivActualElapsedTime",
    "DivArrDelay",
    "DivDistance",
    "Div1Airport",
    "Div1AirportID",
    "Div1AirportSeqID",
    "Div1WheelsOn",
    "Div1TotalGTime",
    "Div1LongestGTime",
    "Div1WheelsOff",
    "Div1TailNum",
    "Div2Airport",
    "Div2AirportID",
    "Div2AirportSeqID",
    "Div2WheelsOn",
    "Div2TotalGTime",
    "Div2LongestGTime",
    "Div2WheelsOff",
    "Div2TailNum",
    "Div3Airport",
    "Div3AirportID",
    "Div3AirportSeqID",
    "Div3WheelsOn",
    "Div3TotalGTime",
    "Div3LongestGTime",
    "Div3WheelsOff",
    "Div3TailNum",
    "Div4Airport",
    "Div4AirportID",
    "Div4AirportSeqID",
    "Div4WheelsOn",
    "Div4TotalGTime",
    "Div4LongestGTime",
    "Div4WheelsOff",
    "Div4TailNum",
    "Div5Airport",
    "Div5AirportID",
    "Div5AirportSeqID",
    "Div5WheelsOn",
    "Div5TotalGTime",
    "Div5LongestGTime",
    "Div5WheelsOff",
    "Div5TailNum",
]
