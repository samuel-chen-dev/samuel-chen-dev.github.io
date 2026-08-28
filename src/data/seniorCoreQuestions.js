function balance(options, answer, index) { const correct = options[answer]; const arranged = options.filter((_, i) => i !== answer); const correctIndex = index % 4; arranged.splice(correctIndex, 0, correct); return { options: arranged, correctIndex }; }
function build(moduleId, levels) { return levels.map(({ topic, rows }, li) => ({ level: li + 1, passScore: 3, questions: rows.map(([question, options, answer, reason], qi) => ({ id: `senior_${moduleId}_${li + 1}_${qi + 1}`, question, ...balance(options, answer, li * 5 + qi), explanation: `本题考查${topic}。${reason}；其他选项不符合句法结构、语域或上下文逻辑。`, type: moduleId === 'grammar' ? 'grammar' : 'choice' })) })); }

const grammar = [
{topic:'复杂时态与语态',rows:[
['By next June, the bridge ___ for three years.',['will have been built','will have been under construction','has been constructed','is being built'],1,'for three years强调截至将来持续的状态'],
['When the data finally arrived, researchers ___ the model for months.',['tested','have tested','had been testing','would test'],2,'数据到达前持续数月，用过去完成进行时'],
['The manuscript ___ before the editor raised another concern.',['revised','has revised','was revising','had been revised'],3,'修改先于过去动作且主语承受动作'],
['This time tomorrow, we ___ the results to the committee.',['will be presenting','present','have presented','presented'],0,'this time tomorrow表示将来某时正在进行'],
['No decision ___ until all evidence has been reviewed.',['made','will be made','is making','has made'],1,'决定被作出，且主句表示将来'] ]},
{topic:'非谓语综合',rows:[
['___ from a distance, the building appears perfectly round.',['Viewing','To view','Viewed','Having viewed'],2,'建筑被观察，用过去分词作状语'],
['The scientist is believed ___ the method independently.',['developing','developed','develop','to have developed'],3,'believe动作发生前已完成，用完成不定式'],
['___ the warning, they might have avoided the loss.',['Having followed','Followed','To follow','Following'],0,'先遵循警告，再可能避免损失'],
['There is no point ___ over a decision already made.',['argue','arguing','to argue','argued'],1,'there is no point doing为固定结构'],
['The question ___ at tomorrow’s meeting concerns data privacy.',['discussing','discussed','to be discussed','having discussed'],2,'表示将要被讨论'] ]},
{topic:'定语从句进阶',rows:[
['The laboratory has developed a sensor, the accuracy of ___ exceeds expectations.',['that','which','whose','whom'],1,'介词后指物用which'],
['She reached a point ___ she had to reconsider her assumptions.',['which','that','where','when'],2,'point作抽象地点，关系词作状语'],
['The year 2020, ___ many routines changed, reshaped online learning.',['which','where','that','when'],3,'关系词在非限制从句中作时间状语'],
['Students who question evidence, ___ rather than merely memorize, learn deeply.',['as they do','which they do','that do','what they do'],0,'as引导非限制性定语从句，意为正如他们所做'],
['The books, many of ___ are out of print, remain influential.',['that','which','them','whose'],1,'介词of后用which指物'] ]},
{topic:'名词性从句',rows:[
['___ the policy will reduce inequality remains uncertain.',['If','That','Whether','What'],2,'主语从句表达是否且不可用if置首'],
['The issue is not who proposed it but ___ it can work.',['that','what','where','whether'],3,'表语从句表达是否可行'],
['Evidence suggests ___ sleep plays a role in memory.',['that','whether','what','how'],0,'suggests后接陈述事实的that从句'],
['We should focus on ___ the evidence actually shows.',['that','what','whether','which'],1,'what在宾语从句中作shows的宾语'],
['___ surprised the team was the speed of recovery.',['That','Whether','What','Why did'],2,'what引导主语从句并作其主语'] ]},
{topic:'状语从句省略',rows:[
['When ___ about the result, she remained cautious.',['asking','asked','to ask','having asked'],1,'主语she与ask为被动关系，省略she was'],
['If ___ properly, the device can last ten years.',['maintaining','maintained','maintain','to maintain'],1,'device被维护，省略it is'],
['Though ___ by the setback, he continued the project.',['discouraging','discourage','discouraged','to discourage'],2,'he感到受挫，省略he was'],
['While ___ abroad, she developed an interest in translation.',['studying','studied','to study','study'],0,'主从句主语一致，省略she was'],
['Unless ___ otherwise, submit the form electronically.',['instructing','instructed','to instruct','instruct'],1,'省略you are instructed'] ]},
{topic:'虚拟语气',rows:[
['If I ___ the warning, I would have acted differently.',['had understood','understood','would understand','have understood'],0,'与过去事实相反，从句用过去完成时'],
['The doctor recommended that he ___ a second opinion.',['sought','seeks','seek','would seek'],2,'recommend后的从句用should加原形，should可省'],
['It is time we ___ the long-term consequences.',['consider','have considered','will consider','considered'],3,'it is time后用过去式表达虚拟'],
['Without your support, the project ___ last year.',['would fail','would have failed','failed','had failed'],1,'without替代过去虚拟条件'],
['I wish the discussion ___ more constructive now.',['were','is','has been','will be'],0,'对现在愿望用过去式，be常用were'] ]},
{topic:'倒装与强调',rows:[
['Only after the report was published ___ the error.',['they noticed','did they notice','they did notice','noticed they'],1,'only加状语置首引起部分倒装'],
['It was the unexpected result ___ changed the direction of research.',['who','what','that','where'],2,'强调句结构It was...that'],
['Not until midnight ___ the rescue team return.',['had','was','has','did'],3,'not until置首，主句部分倒装'],
['Rarely ___ such cooperation across departments.',['have we seen','we have seen','did we saw','we saw'],0,'否定频率副词置首引起倒装'],
['It is evidence, rather than confidence, ___ should guide the choice.',['who','that','what','where'],1,'强调句强调主语'] ]},
{topic:'独立主格',rows:[
['The meeting ___ over, everyone returned to work.',['was','being','is','be'],1,'名词加现在分词构成独立主格'],
['Weather ___, the launch will take place tomorrow.',['permitted','permits','permitting','to permit'],2,'weather permitting为固定独立主格'],
['All tasks ___, the team took a short break.',['completing','complete','to complete','completed'],3,'任务被完成，用过去分词'],
['There ___ no further questions, the chair ended the session.',['being','was','had','is'],0,'there being构成独立主格'],
['The lights ___, the room looked welcoming.',['turning on','on','were on','to be on'],1,'名词加副词构成独立主格'] ]},
{topic:'长难句综合',rows:[
['What matters is not ___ quickly information is found, but whether it is evaluated critically.',['what','that','how','which'],2,'how修饰quickly引导表语从句'],
['The proposal, ___ ambitious, offers a framework worth testing.',['because','unless','while','despite'],2,'while引导让步省略句'],
['___ appears to be a simple choice often involves competing values.',['What','That','Which','Whether'],0,'what引导主语从句并在从句作主语'],
['The more evidence we examine, ___ our conclusion becomes.',['the most reliable','the more reliable','more reliable','reliably'],1,'the more...the more比较结构'],
['Had the variables not been controlled, the result ___ meaningless.',['is','was','will be','would have been'],3,'省略if的过去虚拟倒装，主句用would have done'] ]}
];

const vocabulary = [
{topic:'高中构词法',rows:[
['“Interdisciplinary” most nearly means involving ___.',['several fields','one person','past events','private opinions'],0,'inter-表示之间，disciplinary指学科'],
['The noun form of “resilient” is ___.',['resist','resilience','resiliently','residency'],1,'resilient对应名词resilience'],
['“Overestimate” means ___.',['estimate again','estimate jointly','estimate too highly','avoid estimating'],2,'over-表示过度'],
['A policy that cannot be sustained is ___.',['dissustained','nonsustain','antisustain','unsustainable'],3,'un-与-able构成否定形容词'],
['The suffix in “modernize” means ___.',['make or become','without','a person','before'],0,'-ize表示使成为'] ]},
{topic:'学术动词',rows:[
['The findings ___ the assumption that exercise aids concentration.',['destroy','support','borrow','announce'],1,'support an assumption表示支持假设'],
['The study seeks to ___ a relationship between diet and sleep.',['invent','remove','establish','divide'],2,'establish a relationship是学术搭配'],
['Researchers must ___ potential sources of error.',['celebrate','repeat','ignore','identify'],3,'identify sources of error表示识别误差来源'],
['The author ___ between correlation and causation.',['distinguishes','persuades','translates','depends'],0,'distinguish between为固定搭配'],
['Later evidence may ___ the original theory.',['perform','challenge','deliver','observe'],1,'challenge a theory表示质疑理论'] ]},
{topic:'抽象名词',rows:[
['Public ___ of the issue increased after the report.',['aware','awarely','awareness','awaken'],2,'public awareness为公共意识'],
['The plan requires cooperation and mutual ___.',['respectful','respectably','respected','respect'],3,'形容词mutual修饰名词respect'],
['Scientific progress depends on intellectual ___.',['curiosity','curious','curiously','curiousness'],0,'intellectual curiosity为求知欲'],
['The policy improved access but not necessarily ___.',['equal','equality','equally','equalize'],1,'与access并列需名词equality'],
['There is growing ___ about data privacy.',['concerned','concerning','concern','concerns about'],2,'growing concern为日益担忧'] ]},
{topic:'形近词辨析',rows:[
['The new rule may ___ how students use the platform.',['effect','affect','afford','effort'],1,'affect作动词表示影响'],
['Please ___ that all sources are listed.',['insure','assure','ensure','secure'],2,'ensure that表示确保'],
['The weather can be ___, so carry extra clothing.',['variable','various','variety','varying from'],0,'variable表示易变的'],
['The medicine had no ___ effect.',['adverse','averse','advert','advice'],0,'adverse effect为不良影响'],
['Her explanation was clear and ___.',['comprehensive','comprehensible','comprehension','comprehend'],1,'comprehensible表示可理解的'] ]},
{topic:'搭配与语域',rows:[
['The committee will ___ a formal investigation.',['do','conduct','make','play'],1,'正式语域用conduct an investigation'],
['The evidence is ___ consistent with the hypothesis.',['heavy','deep','broadly','loudly'],2,'broadly consistent为学术搭配'],
['Please ___ from using informal language in the report.',['prevent','escape','avoid','refrain'],3,'refrain from doing为正式表达'],
['The change had a ___ impact on rural communities.',['significant','tall','sharp sound','wide loudly'],0,'significant impact为正式搭配'],
['The author ___ attention to a neglected issue.',['pays','draws','takes','brings at'],1,'draw attention to为固定搭配'] ]},
{topic:'熟词生义',rows:[
['In “address the problem”, “address” means ___.',['write an address','deliver mail','deal with','speak loudly'],2,'address作动词表示处理问题'],
['In “a sound argument”, “sound” means ___.',['noisy','musical','quiet','well-founded'],3,'sound修饰argument表示可靠合理'],
['In “subject to change”, “subject” means ___.',['likely to experience','school course','a person studied','topic only'],0,'be subject to表示可能受影响'],
['In “a novel approach”, “novel” means ___.',['fictional','new and original','lengthy','literary'],1,'修饰approach表示新颖的'],
['In “capital punishment”, “capital” means ___.',['financial','main city','involving death','uppercase'],2,'capital punishment指死刑'] ]},
{topic:'逻辑衔接',rows:[
['The sample was small; ___, the findings require caution.',['otherwise','meanwhile','similarly','consequently'],3,'后句是前因导致的结果'],
['The method is expensive. ___, it produces highly accurate results.',['Nevertheless','Therefore','For instance','Likewise'],0,'前后为让步转折'],
['The first study measured speed; ___, the second examined accuracy.',['therefore','by contrast','as a result','in addition to'],1,'两项研究形成对比'],
['Several factors matter, ___ cost, access and reliability.',['instead','otherwise','namely','however'],2,'namely引出具体列举'],
['The evidence remains limited. ___, no firm conclusion should be drawn.',['Similarly','Meanwhile','For example','Accordingly'],3,'accordingly表示据此'] ]},
{topic:'主题语块',rows:[
['Protecting biodiversity requires us to ___ habitat loss.',['tackle','perform','borrow','translate'],0,'tackle habitat loss为环境主题语块'],
['Digital literacy helps users ___ credible sources.',['take after','distinguish between','come across as','look forward to'],1,'distinguish between credible sources'],
['Communities must ___ climate-related risks.',['make sense','pay back','adapt to','bring out'],2,'adapt to risks表示适应风险'],
['Education can ___ social mobility.',['take notes','draw pictures','lose sight of','promote'],3,'promote social mobility为教育主题语块'],
['Governments should ___ public concerns transparently.',['respond to','result from','belong to','depend at'],0,'respond to concerns为治理语块'] ]},
{topic:'高考语境综合',rows:[
['Her account was vivid yet carefully ___ by historical evidence.',['entertained','grounded','escaped','removed'],1,'be grounded in evidence表示以证据为基础'],
['The discovery was not accidental but the ___ of years of work.',['entrance','permission','outcome','direction'],2,'outcome表示长期工作的结果'],
['A good model simplifies reality without ___ essential detail.',['creating','measuring','explaining','sacrificing'],3,'without sacrificing表示不牺牲关键信息'],
['The speech ___ with students because it reflected their experience.',['resonated','competed','separated','hesitated'],0,'resonate with表示引起共鸣'],
['The policy’s benefits must be weighed ___ its costs.',['through','against','inside','beside'],1,'weigh against表示权衡利弊'] ]}
];

export const SENIOR_GRAMMAR_QUESTIONS = build('grammar', grammar);
export const SENIOR_VOCABULARY_QUESTIONS = build('vocabulary', vocabulary);
