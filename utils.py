import os
import json
import time
from tqdm import tqdm


def extract_sql_from_json_predictions(filepath, dataset="spider", output_dir="default", tag=""):
    '''
    Args:
        filepath：picard预测结果文件
        dataset：评估的数据集，限定在"spider","sparc","cosql"
        output_dir：输出文件目录，默认./output/
        tag：添加到文件名中的自定义标签信息
        
    Returns:
        predictions：列表形式的返回结果，每一行对应于txt文件中的一行内容
    '''
    predictions = []
    json_text = json.load(open(filepath, "r"))
    if output_dir == "default":
        dirs = "./output"
    else:
        dirs = output_dir
    time_text = time.strftime('%Y%m%d_%H%M%S', time.localtime())
    filename = "output_"+dataset+"_"+tag+"_"+time_text+".txt"
    if not os.path.exists(dirs):
        os.makedirs(dirs)
    filepath = os.path.join(dirs, filename)
    print("filepath: ", filepath)
    if dataset == "spider":
        with open(filepath, "w") as f:
            for item in tqdm(json_text):
                pred = item['prediction']
                text = pred.split("|", 1)[-1].strip()
                predictions.append(text)
                f.write(text+"\n")
    elif dataset in set(["sparc", "cosql"]):
        with open(filepath, "w") as f:
            for item in tqdm(json_text[1:]):
                pred = item['prediction']
                turn_idx = item["turn_idx"]
                if turn_idx == -1:
                    text = ""
                else:
                    text = pred.split("|", 1)[-1].strip()
                predictions.append(text)
                f.write(text+"\n")
    return predictions, filepath


def get_default_parameters(base_dir="/home/jxqi/text2sql/error_analysis/data/"):
    '''
    Args:
        base_dir：存放这几个数据库的数据文件目录
        
    Returns:
        default_paramerters：字典索引的各个测试文件（夹）路径
    '''
    datasets = ["spider", "sparc", "cosql"]
    default_paramerters = dict()

    for d in datasets:
        default_paramerters[d] = dict()
        default_paramerters[d]["db"] = os.path.join(base_dir, d, "database")
        default_paramerters[d]["table"] = os.path.join(base_dir, d, "tables.json")
        default_paramerters[d]["gold"] = os.path.join(base_dir, d, "dev_gold.txt")
        
    return default_paramerters


def get_eval_result_from_txt_file(txt_filepath, dataset="spider", etype="match", test_suit_path="/home/jxqi/text2sql/test-suite-sql-eval"):
    '''
    Args:
        txt_filepath：预测的txt格式的结果，其中每一行为一个
        dataset：评估的数据集，限定在"spider","sparc","cosql"
        etype：评估使用的metric，限定于"match","exec"
        test_suit_path：使用的evaluation程序，默认为官方验证程序/home/jxqi/text2sql/test-suite-sql-eval/evaluation.py
        output_dir：输出文件目录，默认./output/
        tag：添加到文件名中的自定义标签信息
        
    Returns:
        eval_result：评测得到的结果
    '''
    script_path = os.path.join(test_suit_path, "evaluation.py")
    default_paramerters = get_default_parameters()
    cmd = 'python {script} --gold {gold} --pred {pred} --etype {etype} --db {db} --table {table}'.format(
        script = script_path,
        pred = txt_filepath,
        etype = etype,
        db = default_paramerters[dataset]["db"],
        table = default_paramerters[dataset]["table"],
        gold = default_paramerters[dataset]["gold"],
    )
    result = os.popen(cmd).read()
    if etype == "match":
        # result_l = result.split("\n")
        # result = "\n".join(result_l[-40:-35])
        print(result)
    else:
        print(result)
    
    return result
        
    
def convert_text_to_list(text_l):
    '''
    Args:
        text_l：evaluate输出结果
        
    Returns:
        question_result_l: 每个问题的评测结果组成的list，1为正确，0为错误
        iteraction_result_l: 每个交互的评测结果组成的list，1为正确，0为错误
        turn_info_l：每个交互的评测结果的详细信息
    '''
    question_num, iteraction_num = 0, 0                  # 总计的问题数目， 交互次数
    turn_num_l, iteraction_index_l = [], []              # 每次交互的轮数， 交互次数的index
    question_result_l, iteraction_result_l = [], []      # 问题的预测结果， 交互的预测结果
    turn_info_l = []
    for i, text in enumerate(text_l):
        if "th prediction" in text or "joint_all" in text:
            iteraction_num += 1
            iteraction_index_l.append(i)
            if i>0:
                turn_num_l.append(int((iteraction_index_l[-1]-iteraction_index_l[-2])/4))
    for i, text in enumerate(text_l):
        if text == "Right":
            question_result_l.append(1)
        if text == "Wrong":
            question_result_l.append(0)
    tmp_question_result_l = question_result_l[:]
    tmp_text_l = text_l[:]
    for i, length in enumerate(turn_num_l):
        interaction_question_result_l = tmp_question_result_l[:length]
        turn_text_l = tmp_text_l[:length*4+1]
        turn_info = turn_text_l
        turn_info_l.append(turn_info)
        # print(i, interaction_question_result_l)
        # print(i, turn_text)
        if 0 in interaction_question_result_l:
            iteraction_result_l.append(0)
        else:
            iteraction_result_l.append(1)
        tmp_question_result_l = tmp_question_result_l[length:]
        tmp_text_l = tmp_text_l[length*4+1:]
    return question_result_l, iteraction_result_l, turn_info_l


def result_compare(baseline_text_l, text_l):
    '''
    
    '''
    baseline_question_result_l, baseline_iteraction_result_l, baseline_turn_info_l = convert_text_to_list(baseline_text_l)
    question_result_l, iteraction_result_l, turn_info_l = convert_text_to_list(text_l)
    dev_context_l = get_all_context_from_dev()
    both_right_l, both_wrong_l, baseline_right_l, right_l = [], [], [], []
    for i, (context, base_result, result) in enumerate(zip(dev_context_l, baseline_iteraction_result_l, iteraction_result_l)):
        item = (context, baseline_turn_info_l[i], turn_info_l[i])
        if base_result == 1:
            if result ==1:           
                both_right_l.append(item)
            else:
                baseline_right_l.append(item)
        else:
            if result ==1:
                right_l.append(item)
            else:
                both_wrong_l.append(item)
                
    result_dict = {
        "both_right_l": both_right_l,
        "both_wrong_l": both_wrong_l,
        "baseline_right_l": baseline_right_l,
        "right_l": right_l,
    }
                
    return result_dict


def get_all_context_from_dev(dev_filepath="/home/jxqi/text2sql/error_analysis/data/sparc/dev.json"):
    '''
    Args:
        dev_filepath: dev.json文件的路径
        
    Returns:
        dev_context_l: 存放每一个interaction的context的列表，其中每个元素为一个字典，包括"goal"为交互目标，"interactions"为对话上下文
    '''
    dev = json.load(open(dev_filepath, "r"))
    dev_context_l = []
    for item in dev:
        goal = item["final"]["utterance"]
        interactions = [i["utterance"] for i in item["interaction"]] 
        context = {
            "goal": goal,
            "interactions": interactions
        }
        dev_context_l.append(context)
        
    return dev_context_l


def analysis_compare(result_dict, part="right_l", mode="refined"):
    '''
    Args:
        result_dict: 经过result_compare函数处理后的结果
        part: 要展示的哪部分类型，默认为"right_l"，即我们模型比baseline模型预测的好的部分
        mode: 打印类型，默认为"refined"
        
    Returns:
        
    '''
    part_dict = result_dict[part]
    if mode == "refined":
        for i in range(len(part_dict)):
            print("===="*36)
            print("===="*36)
            print("Interaction context:")
            print("Goal: ", part_dict[i][0]["goal"])
            print("----"*36) 
            baseline_result = part_dict[i][1][1:]
            result = part_dict[i][2][1:]
            # print(result)
            # print(baseline_result)
            for i, interaction in enumerate(part_dict[i][0]["interactions"]):
                print("Question #", str(i), ": ", interaction)
                index = 4*i+1
                base_question_result = baseline_result[index-1]
                # print(base_question_result)
                if "Wrong" in base_question_result :
                    print("^^^^^^^^^^^^^^^^^^^^^^^")
                    print("^^^^^Please note: ^^^^^")
                print("Baseline predition:   ", baseline_result[index].replace("easy pred: ", "").replace("medium pred: ", "").replace("hard pred: ", "").replace("extra pred: ", ""))
                print("Our method predition: ", result[index].replace("easy pred: ", "").replace("medium pred: ", "").replace("hard pred: ", "").replace("extra pred: ", ""))
                if "Wrong" in base_question_result :
                    print("^^^^^^^^^^^^^^^^^^^^^^^")
                print()

            print()
            
    else:
        for i in range(len(part_dict)):
            print("===="*36)
            print("===="*36)
            print("Interaction context:")
            print("Goal: ", part_dict[i][0]["goal"])
            print("----"*36)

            print("interactions: \n", "\n".join(part_dict[i][0]["interactions"]))
            print("----"*36)
            print()

            print("Baseline:")
            print("\n".join(part_dict[i][1]))

            print("Our method:")
            print("\n".join(part_dict[i][2]))
            print("===="*36)
            print("===="*36)
            print()
