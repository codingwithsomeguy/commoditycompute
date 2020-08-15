import os
import json
import pickle
from pprint import pprint

import boto3
from jsonpath_ng import jsonpath, parse

CACHE_DIR = os.path.join("..", "cache")
LUT_CACHE_FILE = os.path.join(CACHE_DIR, "lut.json")
ONLY_PRICING_API_REGION = "us-east-1"


# TODO: allow returning the cache without the function call
def cache_api_result(api_result, cache_filename):
    os.makedirs(CACHE_DIR, exist_ok=True)
    full_cache_filename = os.path.join(CACHE_DIR, cache_filename)

    pickle.dump(api_result, open(full_cache_filename, "wb"))

    return api_result


def skip_next(node):
    the_first_key = list(node.keys())[0]
    return node[the_first_key]


def instance_pricing():
    pricing = boto3.client("pricing", region_name=ONLY_PRICING_API_REGION)
    prices = cache_api_result(
        pricing.get_products(
            ServiceCode="AmazonEC2",
            MaxResults=100,
            Filters=[
                {
                    "Type": "TERM_MATCH",
                    "Field": "instanceType",
                    "Value": "t3.micro",
                },
                {
                    "Type": "TERM_MATCH",
                    "Field": "operatingSystem",
                    "Value": "Linux",
                },
                {
                    "Type": "TERM_MATCH",
                    "Field": "processorArchitecture",
                    "Value": "64-bit",
                },
                {
                    "Type": "TERM_MATCH",
                    "Field": "intelAvxAvailable",
                    "Value": "Yes",
                },
                # {"Type": "TERM_MATCH", "Field": "tenancy", "Value": "On-Demand"},
            ],
        ),
        "pricing.pkl",
    )

    priceperunit_expr = parse("$.priceDimensions.*.pricePerUnit.USD")
    description_expr = parse("$.priceDimensions.*.description")
    vcpu_expr = parse("$.product.attributes.vcpu")
    instancetype_expr = parse("$.product.attributes.instanceType")
    usagetype_expr = parse("$.product.attributes.usagetype")

    # TODO: just use jsonpath, remove manual navigation
    for line in prices["PriceList"]:
        parsed = json.loads(line)

        vcpu = int(vcpu_expr.find(parsed)[0].value)
        usage_type = usagetype_expr.find(parsed)[0].value
        instance_type = instancetype_expr.find(parsed)[0].value

        if "terms" not in parsed or "OnDemand" not in parsed["terms"]:
            continue
        if parsed["product"]["attributes"]["preInstalledSw"] != "NA":
            continue
        the_offer = parsed["terms"]["OnDemand"][
            list(parsed["terms"]["OnDemand"].keys())[0]
        ]

        # TODO: fix the IndexError, remove try
        try:
            price_per_unit = float(priceperunit_expr.find(the_offer)[0].value)
            if price_per_unit < 0.0000001:
                # TODO: filter out these 0 cost instances in the api call
                continue
            description = description_expr.find(the_offer)[0].value
            print(usage_type, vcpu, instance_type, price_per_unit)
            # description data seems encoded in usage_type
        # TODO: don't do this
        except IndexError:
            pass


def spot_pricing(region_name, lut):
    # TODO: consider: t3.micro doesn't have any vcpu; sub ecus?
    INSTANCE_TYPE = "t3.micro"

    ec2 = boto3.client("ec2", region_name=region_name)
    spot_history = cache_api_result(
        ec2.describe_spot_price_history(
            InstanceTypes=[INSTANCE_TYPE],
            MaxResults=10,
            ProductDescriptions=["Linux/UNIX"],
        ),
        "spothistory.pkl",
    )

    for spot in spot_history["SpotPriceHistory"]:
        vcpu = 0
        if (
            region_name in lut
            and spot["InstanceType"] in lut[region_name]
            and "vcpu" in lut[region_name][spot["InstanceType"]]
        ):
            vcpu = int(lut[region_name][spot["InstanceType"]]["vcpu"])
        print(
            spot["AvailabilityZone"],
            vcpu,
            spot["InstanceType"],
            spot["SpotPrice"],
            spot["Timestamp"].timestamp(),
        )


def region_instance_descriptions(region_name):
    ec2 = boto3.client("ec2", region_name=region_name)
    described_instances = cache_api_result(
        ec2.describe_instance_types(), "id.pkl"
    )

    result = {}
    for instance_type in described_instances["InstanceTypes"]:
        all_arch = instance_type["ProcessorInfo"]["SupportedArchitectures"]
        for arch in all_arch:
            memory_MiB = instance_type["MemoryInfo"]["SizeInMiB"]
            instance_type_name = instance_type["InstanceType"]
            vcpus = instance_type["VCpuInfo"]["DefaultVCpus"]
            result[instance_type_name] = {
                "vcpu": vcpus,
                "mem": memory_MiB,
                "arch": arch,
            }

    return result


def get_all_usable_regions():
    ec2 = boto3.client("ec2")
    regions = cache_api_result(
        ec2.describe_regions(AllRegions=False), "regions.pkl"
    )

    return [region["RegionName"] for region in regions["Regions"]]


def build_instance_description_lut():
    regions = get_all_usable_regions()
    # print(regions)

    lut = {}

    for region in regions:
        print("checking region", region)
        lut[region] = region_instance_descriptions(region)

    return lut


def main():
    lut = None
    # TODO: tie this in with caching
    if os.path.exists(LUT_CACHE_FILE):
        # TODO: expire this if it's too old
        lut = json.load(open(LUT_CACHE_FILE))
    else:
        lut = build_instance_description_lut()
        json.dump(lut, open(LUT_CACHE_FILE, "w"))

    instance_pricing()
    spot_pricing("us-east-2", lut)


if __name__ == "__main__":
    main()
    # TODO: get all regions, get most types, rolling average for spot
    # TODO: benchmark tool (FAHBench?) for different instance types.  Cost per amount of work done
    # TODO: GPU version... later
    # TODO: figure out how to preserve work across instance termination - manually handle attaching EBS drives? s3 storage? need to know live (attached / running) vs. still going WU.  COW / ACID drive semantics? FUSE?
    # TODO: https://github.com/cormac85/fahlogstats
    # TODO: equivalence - how many m4s can do the same PointsPerHour (PPH) as  one GPU instance (vs. the price diff)
