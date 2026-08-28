function balance(options, answer, index) {
  const correct = options[answer];
  const arranged = options.filter((_, i) => i !== answer);
  const correctIndex = index % 4;
  arranged.splice(correctIndex, 0, correct);
  return { options: arranged, correctIndex };
}

function build(moduleId, levels, listeningCount) {
  return levels.map(([topic, rows], li) => ({ level: li + 1, passScore: 3, questions: rows.map(([question, audioText, options, answer], qi) => ({ id: `zhongkao_${moduleId}_${li + 1}_${qi + 1}`, question, ...(audioText ? { audioText } : {}), ...balance(options, answer, li * 5 + qi), explanation: `本题考查${topic}。${options[answer]}最准确地完成信息交换、语用回应或综合表达；其他选项存在信息遗漏、逻辑偏差或语气不当。`, type: qi < listeningCount ? 'listening' : 'choice' })) }));
}

const speaking = [
  ['信息询问', [
    ['What information does the caller need?','Could you tell me whether the museum opens on public holidays?',['Holiday opening hours','Ticket prices','The museum address','The newest exhibition'],0],
    ['Why does the student ask again?','Sorry, did you say the deadline was May thirteenth or May thirtieth?',['To change the deadline','To confirm the date','To cancel the task','To ask for a topic'],1],
    ['Which question requests precise information?','',['Is it good?','What about that?','How long does the workshop last?','Can you tell me something?'],2],
    ['You did not understand one detail. Say:','',['Repeat everything now.','Your answer is unclear.','Never mind all of it.','Could you explain what “online submission” means?'],3],
    ['Which sentence summarizes received information?','',['So I need to register online before Friday, correct?','Registration is a long word.','I may have heard something.','Friday comes before Saturday.'],0],
  ]],
  ['活动邀请', [
    ['What event is Leo inviting Amy to?','Our class is holding a charity book sale this Saturday. Would you like to help?',['A charity book sale','A sports meeting','A science lecture','A birthday dinner'],0],
    ['Why can’t Mia attend the whole event?','I can come after two because I have a music exam in the morning.',['She dislikes the event','She has a morning exam','She must sell books elsewhere','She is ill'],1],
    ['Which invitation includes useful details?','',['Join us sometime.','There is an activity.','Would you join our clean-up at Green Park at nine on Sunday?','You should come.'],2],
    ['Accept and offer help:','',['Fine.','Perhaps I will watch.','You have enough people.','I’d love to. Shall I bring some boxes?'],3],
    ['Decline politely with an alternative:','',['I’m sorry I can’t come, but I can help make posters.','No, it sounds boring.','Do not invite me.','Your time is wrong.'],0],
  ]],
  ['投诉处理', [
    ['What problem does the customer report?','I ordered a blue jacket, but the package contains a black one.',['Wrong color','Wrong size','Late delivery','Damaged box'],0],
    ['What solution does the clerk offer?','We can exchange it today or give you a full refund.',['A discount only','Exchange or refund','A repair next month','No solution'],1],
    ['Which complaint is firm but polite?','',['Your service is terrible!','Fix this now!','I’m afraid the headphones stopped working after one day.','I will tell everyone you cheat.'],2],
    ['A staff member should first say:','',['That is not our fault.','You probably broke it.','Read the rules yourself.','I’m sorry about the problem. Let me check your order.'],3],
    ['Which response confirms resolution?','',['Thank you. A replacement by Friday will solve the problem.','I dislike all shops.','The package is square.','I may complain forever.'],0],
  ]],
  ['学习建议', [
    ['What difficulty does the student have?','I understand new words in class, but I forget them a few days later.',['Remembering vocabulary','Reading aloud','Finding a teacher','Finishing math'],0],
    ['What strategy is suggested?','Review the words in short sentences at increasing intervals.',['Copy each word fifty times','Use spaced review in context','Stop learning new words','Translate every book'],1],
    ['Which advice is specific and practical?','',['Study harder.','Do everything better.','Record yourself reading and compare it with the audio.','Never make mistakes.'],2],
    ['How can you respond to someone’s concern?','',['That is easy for everyone.','You are not talented.','Just ignore it.','I understand why that feels frustrating.'],3],
    ['Which plan includes reflection?','',['Try the method for a week and note what improves.','Buy more notebooks.','Study whenever possible.','Avoid difficult tasks.'],0],
  ]],
  ['文化介绍', [
    ['What custom is explained?','During Spring Festival, younger family members may receive red envelopes with good wishes.',['Giving red envelopes','Eating turkey','Carving pumpkins','Painting eggs'],0],
    ['Why does the speaker mention regional differences?','Traditions vary across China, so one family’s practice may not represent everyone.',['To reject traditions','To avoid overgeneralization','To shorten the talk','To compare prices'],1],
    ['Which introduction explains meaning, not just appearance?','',['The lantern is red.','It is made of paper.','The lantern symbolizes hope and reunion.','Many shops sell it.'],2],
    ['Correct a cultural misunderstanding politely:','',['You know nothing.','That custom is strange.','All Chinese people agree.','Actually, practices differ by region and family.'],3],
    ['Which closing invites dialogue?','',['What traditions are important in your family?','That is the only correct custom.','You should copy this festival.','My culture is better.'],0],
  ]],
  ['环保倡议', [
    ['What action does the speaker propose?','Let’s set up a uniform exchange so students can reuse clothes they have outgrown.',['A uniform exchange','A longer school day','A plastic sale','A new exam'],0],
    ['What evidence supports the proposal?','Last term, over two hundred usable uniforms were thrown away.',['Uniforms are fashionable','Two hundred usable uniforms were discarded','Students dislike exchanges','New clothes are free'],1],
    ['Which slogan connects action and result?','',['Green is a color.','Do something now.','Refill today, reduce plastic tomorrow.','Bottles are useful.'],2],
    ['Which call to action is measurable?','',['Care more.','Protect everything.','Think about nature.','Bring one reusable bottle every school day.'],3],
    ['How should an effective proposal address difficulty?','',['Provide collection points and a cleaning plan.','Ignore questions about cost.','Promise zero problems.','Blame people who disagree.'],0],
  ]],
  ['面试表达', [
    ['What strength does the candidate describe?','I organize tasks carefully and keep the team informed about progress.',['Organization and communication','Artistic talent','Physical strength','Foreign travel'],0],
    ['How does she support her claim?','During the science fair, I created our schedule and adjusted it when materials arrived late.',['By repeating the claim','With a concrete example','By criticizing teammates','With an unrelated award'],1],
    ['Which answer shows self-awareness?','',['I have no weaknesses.','I am better than everyone.','I used to avoid speaking, so I now volunteer to present.','Weaknesses do not matter.'],2],
    ['Asked about failure, you should:','',['Hide the result.','Blame another person.','Change the subject.','Explain what happened and what you learned.'],3],
    ['Which question shows genuine interest?','',['What responsibilities would the student leader have?','Will this be easy?','Do I get a prize?','Can others do my work?'],0],
  ]],
  ['观点辩论', [
    ['What is the speaker’s position?','School uniforms can reduce clothing pressure, although students should have some choice in style.',['Support with limited choice','Complete opposition','No clear opinion','Support without conditions'],0],
    ['What counterargument is acknowledged?','Some people say phones support learning, but unrestricted use can distract students.',['Phones are too expensive','Phones can support learning','Teachers own phones','Distraction never happens'],1],
    ['Which statement uses evidence?','',['Everyone knows it.','I strongly feel it.','Our survey found that 68 percent preferred a later start.','People online agree.'],2],
    ['Disagree respectfully:','',['That is nonsense.','You did not listen.','Only my view matters.','I see your point, but the evidence suggests another result.'],3],
    ['Which conclusion balances values?','',['We need a rule that protects focus while allowing supervised learning use.','Ban everything immediately.','Let everyone do anything.','There is no possible solution.'],0],
  ]],
  ['中考人机对话', [
    ['Where will the speaker volunteer?','I’m going to help at the city library during the summer holiday.',['City library','Children’s hospital','Sports center','Train station'],0],
    ['Why did the speaker choose this work?','I love reading and want younger children to discover good books.',['To earn money','To share a love of reading','To avoid homework','To meet athletes'],1],
    ['Prompt: “How do you prepare for exams?” Choose the complete answer.','',['Very carefully.','Exams are important.','I make a weekly plan, review mistakes and ask for help when needed.','My books are on the desk.'],2],
    ['Prompt: “Describe a person you admire.” Choose the best response.','',['Many people are admirable.','She is a person.','I have met teachers.','I admire my mother because she remains patient and helps others.'],3],
    ['Prompt: “What will you do to improve your school?”','',['I will survey students and propose a quiet study area.','My school is large.','Improvement is good.','Someone should do something.'],0],
  ]],
];

const fun = [
  ['语音陷阱', [
    ['Which word do you hear?','Three.',['three','tree','free','the'],0],
    ['Which word has a different “th” sound?','',['think','this','three','throw'],1],
    ['Which pair contains silent letters?','',['map—pen','rice—nose','knock—write','game—home'],2],
    ['Which word has stress on the second syllable?','',['TEAcher','HAPpy','TAble','reLAX'],3],
    ['Which ending is pronounced /ɪd/?','',['wanted','played','washed','laughed'],0],
  ]],
  ['标语改写', [
    ['Which slogan is clearest?','Please use both sides of every sheet of paper.',['Use both sides, waste less.','Paper exists.','Do not be bad.','Sides are useful.'],0],
    ['Rewrite “Don’t waste water” positively.','',['Water is wet.','Save water, every drop counts.','Waste is wrong always.','You use water.'],1],
    ['Which slogan uses parallel structure?','',['Read books and thinking.','To read, ideas grow.','Read more, think deeper, act wiser.','Books are for reading and thought.'],2],
    ['Which is best for a quiet-zone sign?','',['No person may create any sound whatsoever.','Noise is a thing.','You should know what to do.','Quiet minds at work—please silence phones.'],3],
    ['Which revision avoids blaming readers?','',['Choose reusable cups and help cut waste.','Only careless people use plastic.','Stop being selfish.','You always waste too much.'],0],
  ]],
  ['文化误读', [
    ['What does the speaker correct?','Not every British person drinks tea at exactly five o’clock; habits vary.',['An overgeneralization','A train time','A tea recipe','A school rule'],0],
    ['Which statement avoids stereotyping?','',['All teenagers are alike.','Customs vary among individuals and regions.','One visitor represents a nation.','Every family celebrates identically.'],1],
    ['A polite clarification begins with ___.','',['You are wrong.','That is ridiculous.','That may be true for some people, but...','Everyone knows...'],2],
    ['Which source best checks a cultural claim?','',['One anonymous comment','A comedy scene','A product advertisement','Several reliable sources and lived perspectives'],3],
    ['Culture is best understood as ___.','',['diverse and changing','fixed and uniform','a list of strange habits','only festivals and food'],0],
  ]],
  ['广告辨析', [
    ['What persuasive trick is used?','Nine out of ten stars choose Bright Shoes!',['Unsupported popularity claim','Detailed evidence','Balanced comparison','Safety warning'],0],
    ['Which phrase signals exaggeration?','',['May be useful','The best product ever made','Tested under these conditions','Results can vary'],1],
    ['Which information would make an ad more trustworthy?','',['Larger red letters','A celebrity photo','Verifiable test methods and results','A louder song'],2],
    ['Which sentence separates fact from opinion?','',['Everyone loves it.','It will change your life.','Nothing compares with it.','The battery lasted ten hours in our stated test.'],3],
    ['Before buying, a careful reader should ___.','',['compare independent information','trust emotional language','ignore total cost','share the ad first'],0],
  ]],
  ['新闻核查', [
    ['What warning sign do you hear?','The post makes a dramatic claim but gives no author, date or source.',['Missing source information','Too many interviews','A cautious headline','Published corrections'],0],
    ['What should you check first?','',['Whether friends shared it','The original source','How exciting it sounds','The number of emojis'],1],
    ['Two reliable reports disagree. What next?','',['Choose the shorter one.','Reject both instantly.','Compare evidence and publication dates.','Trust the one you saw first.'],2],
    ['Which headline is most responsible?','',['Miracle Food Ends All Illness!','Scientists Prove Everything!','You Won’t Believe This!','Early Study Suggests a Possible Link'],3],
    ['Reverse image search can help find ___.','',['an image’s earlier context','a writer’s feelings','future events','private passwords'],0],
  ]],
  ['推理谜题', [
    ['Who arrived first?','Amy arrived before Ben. Ben arrived before Chen.',['Amy','Ben','Chen','Cannot know'],0],
    ['A box is not red. It is either red or blue. It is ___.','',['green','blue','red','yellow'],1],
    ['All club members wear badges. Lee has no badge. What follows?','',['Lee leads the club.','Lee lost every badge.','Lee is not a club member under the rule.','Badges are optional.'],2],
    ['The key is in exactly one place: not the bag, desk or drawer. Where is it?','',['Bag','Desk','Drawer','Shelf'],3],
    ['If rain cancels the match and the match was not canceled, then ___.','',['it did not rain under the stated rule','it certainly snowed','the rule is false','the match happened yesterday'],0],
  ]],
  ['双关语', [
    ['Why is the joke funny?','The bicycle couldn’t stand up because it was two-tired.',['“Two-tired” sounds like “too tired”','Bicycles sleep','Two wheels are wrong','Standing is illegal'],0],
    ['A pun usually depends on ___.','',['long grammar rules','similar sounds or multiple meanings','historical dates','formal pronunciation only'],1],
    ['“I used to be a banker, but I lost interest.” Which word has two meanings?','',['used','banker','interest','lost'],2],
    ['Which line contains a pun?','',['The sun is hot.','I read a book.','Banks hold money.','A boiled egg is hard to beat.'],3],
    ['Puns may be difficult to translate because ___.','',['sound and meaning differ across languages','all jokes are secret','grammar cannot be translated','other languages lack humor'],0],
  ]],
  ['中国故事英译', [
    ['Choose the best translation.','愚公移山表现了坚持不懈的精神。',['The story of Yu Gong shows the spirit of perseverance.','Yu Gong moved because mountains spoke.','The mountain tells a short joke.','Persistence means changing homes.'],0],
    ['“端午节是为了纪念屈原” is best translated as:','',['Qu Yuan made every festival.','The Dragon Boat Festival commemorates Qu Yuan.','Dragon boats remember all water.','The festival is a boat.'],1],
    ['Which translation is culturally clear for “春联”?','',['spring papers','red words','Spring Festival couplets','door poems only'],2],
    ['Translate “孔子重视学习与思考的结合。”','',['Confucius only taught reading.','Thinking replaced learning.','Learning was a school.','Confucius valued combining learning with reflection.'],3],
    ['A good cultural translation should ___.','',['convey meaning clearly for the audience','copy every word mechanically','remove cultural context','use the longest vocabulary'],0],
  ]],
  ['综合逃脱任务', [
    ['What is the first code?','Take the first letters of Learn, Observe, Grow, Improve, Continue.',['LOGIC','LOGIN','MAGIC','LIGHT'],0],
    ['A clue says “The room where experiments happen.” Go to the ___.','',['library','laboratory','gym','canteen'],1],
    ['Correct “He don’t know” to get the key verb.','',['doing','did','does','done'],2],
    ['Order the route: first gate, then hall, finally lab.','',['Lab—Hall—Gate','Hall—Gate—Lab','Gate—Lab—Hall','Gate—Hall—Lab'],3],
    ['Final riddle: I become larger the more you take away. I am a ___.','',['hole','shadow','book','clock'],0],
  ]],
];

export const ZHONGKAO_SPEAKING_QUESTIONS = build('speaking', speaking, 2);
export const ZHONGKAO_FUN_QUESTIONS = build('fun', fun, 1);
