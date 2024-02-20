import time
import os
import uuid
import tracemalloc
import json
import sys

#######################
### Test method items

TEST_ITEMS = ['group_transform', 'stack', 'melt', 'filter_bracket', 'filter_query', 'replace', 'sort']

def invoke_group_transform(df):
    _df = df
    # polars not support this so maybe we should treat it as fallback to pandas in this way ;)
    if df_is_polars(_df):
        _df = _df.to_pandas()
    return _df.groupby("object_type")["prop_attack"].transform(lambda att: att.mean() / att.std())

def invoke_stack(df):
    _df = df
    # polars not support this so maybe we should treat it as fallback to pandas in this way ;)
    if df_is_polars(_df):
        _df = _df.to_pandas()
    return _df[['obj_rank', 'rareness_rank']].stack()

def invoke_melt(df):
    sub_df = df[['object_id', 'object_type', 'loc_x', 'loc_y', 'obj_rank', 'rareness_rank']]
    if df_is_polars(df):
        return sub_df.melt(id_vars=['object_id', 'object_type', 'loc_x', 'loc_y'], variable_name='rank_type', value_name='rank_value')
    else:
        return sub_df.melt(id_vars=['object_id', 'object_type', 'loc_x', 'loc_y'], var_name='rank_type', value_name='rank_value')

def invoke_filter_bracket(df):
    _df = df
    # fallback to pandas as we test from the point of view from pandas user
    if df_is_polars(_df):
        _df = _df.to_pandas()
    return _df[(_df["loc_x"] > 1000) & (_df["loc_x"] <= 2000) & (_df["loc_y"] > 2000) & (_df["loc_y"] <= 3000)]

def invoke_filter_query(df):
    _df = df
    # fallback to pandas as we test from the point of view from pandas user
    if df_is_polars(_df):
        _df = _df.to_pandas()
    return _df.query("loc_x > 1000 and loc_x <= 2000 and loc_y > 2000 and loc_y <= 3000")

def invoke_replace(df):
    return df["obj_rank"].str.replace("SSS", "S+")

def invoke_sort(df):
    if df_is_polars(df):
        return df.sort("prop_hp", descending=True)
    else:
        return df.sort_values("prop_hp", ascending=False)

###########
### Funcs

# fireducks.pandas.frame.DataFrame
# polars.dataframe.frame.DataFrame

def test_df(item, df):
    if item not in TEST_ITEMS:
        raise Exception("unknown test item: " + item)

    _func = globals()["invoke_" + item]
    return _func(df)

def _type_cmp(obj, cmp_type):
    return cmp_type in str(type(obj))

def df_is_polars(obj):
    return _type_cmp(obj, "polars.dataframe.frame.DataFrame")

def immediate_exec(df):
    if _type_cmp(df, "fireducks.pandas.frame.DataFrame"):
        df._evaluate()

def init_df_lib(tool_type = 'pandas'):
    p = None
    p_ver = ''
    if tool_type == 'pandas':
        import pandas as p
        p_ver = p.__version__

    elif tool_type == 'fireducks':
        import fireducks.pandas as p
        p_ver = p.__version__

    elif tool_type == 'polars':
        import polars as p
        p_ver = p.__version__

    elif tool_type == 'dask':
        import dask
        import dask.dataframe as p
        p_ver = dask.__version__

    # elif tool_type == 'pyspark':
    #     import pyspark
    #     import pyspark.pandas as p
    #     p_ver = pyspark.__version__

    elif tool_type == 'modin_ray':
        import modin.config as modin_cfg
        modin_cfg.Engine.put("ray")
        import modin.pandas as p
        p_ver = p.__version__

    elif tool_type == 'modin_dask':
        import modin.config as modin_cfg
        modin_cfg.Engine.put("dask")
        import modin.pandas as p
        p_ver = p.__version__

    # elif tool_type == 'modin_unidist':
    #     import modin.config as modin_cfg
    #     import unidist.config as unidist_cfg
    #     modin_cfg.Engine.put('unidist')
    #     unidist_cfg.Backend.put('mpi')
    #     import modin.pandas as p
    #     p_ver = p.__version__

    else:
        raise Exception("unknown tool type: " + tool_type)

    p_toolname = tool_type + " (" + p_ver +  ")"

    return p, p_toolname


def bench(target_lib, target_dataset):
    tracemalloc.start()

    bench_info = {}
    bench_info["id"] = str(uuid.uuid4())
    p, lib_name = init_df_lib(target_lib)
    bench_info["lib_name"] = lib_name
    bench_info["bench_time"] = time.strftime('%Y-%m-%d %H:%M:%S')
    bench_info["dataset_name"] = os.path.basename(target_dataset)
    bench_info["dataset_path"] = os.path.abspath(target_dataset)

    # read_csv
    _t1 = time.time()
    _df = p.read_csv(target_dataset)
    _t2 = time.time()
    bench_info["_dataframe_class"] = str(type(_df))
    bench_info["time"] = {}
    bench_info["time"]["read_csv"] = _t2 - _t1
    bench_info["dataset_size"] = os.path.getsize(target_dataset)

    # in case we need this ;)
    immediate_exec(_df)

    # dropna
    if df_is_polars(_df):
        _t1 = time.time()
        df = _df.drop_nulls()
        _t2 = time.time()
        bench_info["time"]["dropna"] = _t2 - _t1
    else:
        _t1 = time.time()
        df = _df.dropna()
        immediate_exec(df)
        _t2 = time.time()
        bench_info["time"]["dropna"] = _t2 - _t1

    # others
    for test_item in TEST_ITEMS:
        # maybe we need this for jit ;)
        _prepare_df = df.head(1)
        test_df(test_item, _prepare_df)
        # do test
        _t1 = time.time()
        success = True
        try:
            test_df(test_item, df)
        except:
            success = False
            pass
        immediate_exec(df)
        _t2 = time.time()

        if success:
            bench_info["time"][test_item] = _t2 - _t1
        else:
            bench_info["time"][test_item] = -1

    ram_curr, ram_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    bench_info["ram_fin"] = ram_curr
    bench_info["ram_peak"] = ram_peak

    return bench_info

def main():
    argc = len(sys.argv)
    if argc != 4:
        print("bench.py [target lib] [target dataset] [output json]")
        sys.exit(0)

    target_lib = sys.argv[1]
    target_dataset = sys.argv[2]
    output_json = os.path.abspath(sys.argv[3])

    print("benchmarking...")
    ret = bench(target_lib, target_dataset)

    with open(output_json, 'w') as f:
        json.dump(ret, f, ensure_ascii=False, indent=4)

    print(f"done, saved to <{output_json}>")

if __name__ == "__main__":
    main()
