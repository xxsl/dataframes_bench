import uuid
import random
import os
import sys
import time

def _get_random_rank():
    _RANKS = ['SSS', 'S', 'A+', 'A', 'B', 'C', 'D']
    return _RANKS[random.randint(0, len(_RANKS) - 1)]

def generate_gameobj_csv_sample(target_file, scale_factor = 1, with_header = True):
    # generate dummy game objects data 

    if os.path.isfile(target_file):
        os.unlink(target_file)

    with open(target_file, "a") as f:
        if with_header:
            f.write("object_id,object_type,obj_rank,loc_x,loc_y,rareness_rank,prop_hp,prop_mp,prop_attack,prop_defense,prop_magic,flag_1,flag_2,flag_3,flag_4,flag_5\n")

        n = int(scale_factor * 12500000)
        for i in range(n):
            object_id = str(uuid.uuid4())
            object_type = str(random.randint(0,6))
            loc_x = str(random.randint(1,1e6) / 100)
            loc_y = str(random.randint(1,1e6) / 100)
    
            flag_1 = str(random.randint(0, 1))
            flag_2 = str(random.randint(0, 1))
            flag_3 = str(random.randint(0, 1))
            flag_4 = str(random.randint(0, 1))
    
            flag_5 = str(random.randint(0, 1))
            if object_type in ['3', '6']:
                flag_5 = '1'

            obj_rank = _get_random_rank()
            rareness_rank = _get_random_rank()

            prop_hp = str(random.randint(0, 100) * 100)
            prop_mp = str(random.randint(10, 1000000))
            prop_attack = ""
            prop_defense = ""
            prop_magic = ""
    
            if flag_1 == '1':
                prop_attack = str(random.randint(0, 100) * 17)
                prop_defense = str(random.randint(0, 100) * 19)
                prop_magic = str(random.randint(0, 100) * 27)
    
            f.write(f"{object_id},{object_type},{obj_rank},{loc_x},{loc_y},{rareness_rank},{prop_hp},{prop_mp},{prop_attack},{prop_defense},{prop_magic},{flag_1},{flag_2},{flag_3},{flag_4},{flag_5}\n")

def main():
    argc = len(sys.argv)
    if argc != 3:
        print("gen_dataset.py [filename] [scale factor]")
        sys.exit(0)

    param_t = sys.argv[1]
    param_sf = sys.argv[2]
    print(f"generating dummy dataset <{param_t}> (sf={param_sf}) ...")
    random.seed(time.time())
    generate_gameobj_csv_sample(param_t, float(param_sf))
    t_path = os.path.abspath(param_t)
    t_sz = os.path.getsize(t_path)
    print(str(round(t_sz / 1024 / 1024, 2)) + " MB generated to <" + t_path + ">, done")

if __name__ == "__main__":
    main()
