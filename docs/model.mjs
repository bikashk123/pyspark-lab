export const defaults = {left:100,right:64,executors:8,cores:4,memory:16,joinType:'inner',expansion:3,threshold:10,shuffle:100,partitions:200,target:128,skew:1,aqe:true,cache:20,cacheExpansion:2,uses:4,cost:'expensive',udf:false,strategy:'auto'};
export const presets = {
  dimension:{...defaults},
  skewed:{...defaults,left:500,right:20480,shuffle:520,skew:12,cache:100,uses:1},
  repeated:{...defaults,left:40,right:8,shuffle:40,cache:12,uses:6,skew:1.5}
};
export const limits = {left:[.001,100000],right:[.001,100000000],executors:[1,1000],cores:[1,64],memory:[1,1024],expansion:[1,20],threshold:[-1,8192],shuffle:[.001,100000],partitions:[1,10000000],target:[16,2048],skew:[1,100],cache:[.001,100000],cacheExpansion:[.5,20],uses:[1,100]};
const integers = ['executors','cores','partitions','uses'];
export function validate(s){
 const errors={};
 for(const [k,[min,max]] of Object.entries(limits)){if(typeof s[k]!=='number'||!Number.isFinite(s[k])||s[k]<min||s[k]>max||(integers.includes(k)&&!Number.isInteger(s[k]))) errors[k]=`Use ${integers.includes(k)?'a whole number':'a number'} from ${min.toLocaleString('en-US')} to ${max.toLocaleString('en-US')}.`;}
 if(s.threshold<0&&s.threshold!==-1)errors.threshold='Use −1 to disable, or a nonnegative MiB value.';
 if(!['inner','left','right','full'].includes(s.joinType)) errors.joinType='Choose a supported equi-join.';
 if(!['auto','broadcast','merge','hash'].includes(s.strategy)) errors.strategy='Choose a strategy.';
 if(!['cheap','expensive'].includes(s.cost)) errors.cost='Choose recomputation cost.';
 for(const k of ['aqe','udf']) if(typeof s[k]!=='boolean') errors[k]='Choose on or off.';
 return errors;
}
export function analyze(s){
 if(Object.keys(validate(s)).length) throw new Error('Invalid workload');
 const leftMiB=s.left*1024, rightMiB=s.right;
 const buildSide=s.joinType==='left'?'right':s.joinType==='right'?'left':leftMiB<=rightMiB?'left':'right';
 const buildMiB=buildSide==='left'?leftMiB:rightMiB;
 const buildMemory=buildMiB*s.expansion;
 const broadcastBudget=s.memory*1024*.15;
 const eligible=s.joinType!=='full';
 const headroom=buildMemory<=broadcastBudget && buildMemory<8192;
 const conservative=eligible&&headroom&&buildMiB<=512;
 const autoBroadcast=conservative&&s.threshold>=0&&buildMiB<=s.threshold;
 const recommended=conservative?'broadcast':'merge';
 const chosen=s.strategy==='auto'?(autoBroadcast?'broadcast':'merge'):s.strategy;
 const validBroadcast=eligible&&headroom;
 const totalMiB=leftMiB+rightMiB;
 const slots=s.executors*s.cores;
 const shuffleMiB=s.shuffle*1024;
 const suggested=Math.max(1,Math.ceil(shuffleMiB/s.target),Math.min(slots*2,Math.ceil(shuffleMiB/16)));
 const avg=shuffleMiB/s.partitions, proposedAvg=shuffleMiB/suggested;
 const hot=avg*s.skew, taskBudget=s.memory*1024*.30/s.cores;
 const hashTask=buildMiB/s.partitions*s.expansion*s.skew;
 const footprint=s.cache*s.cacheExpansion;
 const cacheBudget=s.executors*s.memory*.30;
 const fits=footprint<=cacheBudget;
 const cacheChoice=s.uses===1?'Skip caching':s.cost==='cheap'?'Benchmark first':fits?'Persist selectively':'Disk-backed persistence';
 const partitionStatus=hot*s.expansion>taskBudget?'Memory pressure':avg<s.target/4?'Many small tasks':avg>s.target*2?'Large partitions':'Reasonable starting point';
 return {buildSide,buildMiB,buildMemory,broadcastBudget,eligible,headroom,conservative,autoBroadcast,recommended,chosen,validBroadcast,totalMiB,slots,suggested,avg,proposedAvg,hot,taskBudget,hashTask,footprint,cacheBudget,fits,cacheChoice,partitionStatus,exchangeMiB:chosen==='broadcast'?0:totalMiB,fanoutMiB:buildMiB*s.executors};
}
export function makeCode(s,m){
 const lines=['# Planning template for Spark SQL / PySpark 3.5+ batch DataFrames.', '# Replace left_df, right_df and "join_key" with your filtered/projected data.', '# Hints are requests; verify the final executed plan in Spark UI.', 'from pyspark.sql import functions as F', '', `spark.conf.set("spark.sql.adaptive.enabled", "${s.aqe}")`, `spark.conf.set("spark.sql.shuffle.partitions", "${s.partitions}")`, `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", ${s.threshold<0?-1:Math.round(s.threshold*1024*1024)})`];
 if(s.aqe) lines.push('spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")', 'spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")', 'spark.conf.set("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false")', `spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", ${Math.round(s.target*1024*1024)})`);
 lines.push('', '# These inputs are assumed to be a sortable-key equi-join.');
 let l='left_df',r='right_df';
 if(m.chosen==='broadcast'){
   if(!m.validBroadcast) lines.push('# Broadcast is unsupported or exceeds this lab\'s memory guardrail.', '# Keep the unhinted join until you have measured build-side memory.');
   else if(s.strategy==='broadcast'){if(m.buildSide==='left') l='F.broadcast(left_df)'; else r='F.broadcast(right_df)'; if(!m.autoBroadcast) lines.push('# Explicit broadcast bypasses the auto threshold. Check driver + executor memory.');}
 }else if(s.strategy==='merge') {l='left_df.hint("merge")';r='right_df.hint("merge")';}
 else if(s.strategy==='hash') {if(m.buildSide==='left') l='left_df.hint("shuffle_hash")';else r='right_df.hint("shuffle_hash")'; lines.push('# Check largest build partition and concurrent hash maps before forcing this hint.');}
 lines.push(`result = ${l}.join(${r}, "join_key", "${s.joinType==='full'?'full_outer':s.joinType}")`, 'result.explain(mode="formatted")', '', `# Separate experiment: try ${m.suggested} initial shuffle partitions`, '# for the modeled downstream shuffle stage; change BEFORE constructing it.', '# Input scan partitions and output files are separate controls.');
 if(s.uses>1&&s.cost==='expensive') lines.push('', 'from pyspark import StorageLevel', '# Replace result with the intermediate whose size you modeled in the cache tab.', '# Cache only the reduced intermediate shared by the repeated actions.', 'cached = result.persist(StorageLevel.MEMORY_AND_DISK_DESER)', 'try:', '    cached.count()  # Materialize once; include this cost in the benchmark.', '    # Run your repeated actions against cached here.', '    pass', 'finally:', '    cached.unpersist()');
 else lines.push('', '# Leave caching off initially; compare total time across all repeated actions.');
 if(s.udf) lines.push('', '# Prefer built-in column expressions over Python UDFs when equivalent.', '# If Python is necessary, test Arrow/vectorized APIs and batch memory.');
 return lines.join('\n');
}
