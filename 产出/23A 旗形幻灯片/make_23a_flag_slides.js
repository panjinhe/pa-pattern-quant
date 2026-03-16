"use strict";

const path = require("path");
const PptxGenJS = require("pptxgenjs");
const { autoFontSize } = require("./pptxgenjs_helpers/text");
const {
  warnIfSlideHasOverlaps,
  warnIfSlideElementsOutOfBounds,
} = require("./pptxgenjs_helpers/layout");

const pptx = new PptxGenJS();
pptx.layout = "LAYOUT_WIDE";
pptx.author = "OpenAI Codex";
pptx.company = "pa-pattern-quant";
pptx.subject = "阿布课程 23A 旗形";
pptx.title = "23A 旗形：最终旗形与失败突破";
pptx.lang = "zh-CN";
pptx.theme = {
  headFontFace: "Microsoft YaHei",
  bodyFontFace: "Microsoft YaHei",
  lang: "zh-CN",
};

const OUT = path.join(__dirname, "23A-旗形-最终旗形与失败突破-交付版.pptx");
const FONT = "Microsoft YaHei";
const COLORS = {
  paper: "F5F0E7",
  ink: "1F2B33",
  muted: "5A6670",
  rust: "B85C38",
  clay: "D98B5F",
  teal: "2F7A78",
  moss: "5E8B63",
  red: "A63D40",
  gold: "C89B2A",
  cream: "FFF9F1",
  sand: "E7DCC8",
  white: "FFFFFF",
  slate: "D8DEE3",
};

function addCanvas(slide, accent, label, index) {
  slide.background = { color: COLORS.paper };
  slide.addShape(pptx.ShapeType.rect, {
    x: 0,
    y: 0,
    w: 13.333,
    h: 7.5,
    line: { color: COLORS.paper, transparency: 100 },
    fill: { color: COLORS.paper },
  });
  slide.addShape(pptx.ShapeType.rect, {
    x: 0,
    y: 0,
    w: 13.333,
    h: 0.38,
    line: { color: accent, transparency: 100 },
    fill: { color: accent },
  });
  slide.addText(label, {
    x: 0.55,
    y: 0.12,
    w: 2.8,
    h: 0.14,
    fontFace: FONT,
    fontSize: 11,
    bold: true,
    color: COLORS.white,
    margin: 0,
  });
  slide.addText(String(index).padStart(2, "0"), {
    x: 12.4,
    y: 0.1,
    w: 0.45,
    h: 0.16,
    fontFace: FONT,
    fontSize: 11,
    bold: true,
    align: "right",
    color: COLORS.white,
    margin: 0,
  });
  slide.addText("阿布课程语音转文字 / 23A 旗形", {
    x: 0.55,
    y: 7.12,
    w: 4.2,
    h: 0.14,
    fontFace: FONT,
    fontSize: 9.5,
    color: COLORS.muted,
    margin: 0,
  });
}

function fitText(text, box, extra = {}) {
  return autoFontSize(text, FONT, {
    x: box.x,
    y: box.y,
    w: box.w,
    h: box.h,
    margin: 0.06,
    minFontSize: 11,
    maxFontSize: 24,
    fontSize: extra.fontSize || 20,
    valign: "mid",
    breakLine: false,
    color: COLORS.ink,
    ...extra,
  });
}

function addTitle(slide, kicker, title, subtitle, accent) {
  slide.addText(kicker, {
    x: 0.7,
    y: 0.72,
    w: 2.7,
    h: 0.22,
    fontFace: FONT,
    fontSize: 12,
    bold: true,
    color: accent,
    margin: 0,
  });
  slide.addText(title, {
    x: 0.7,
    y: 0.98,
    w: 8.9,
    h: 0.8,
    fontFace: FONT,
    fontSize: 25,
    bold: true,
    color: COLORS.ink,
    margin: 0,
  });
  if (subtitle && subtitle.trim()) {
    slide.addText(subtitle, fitText(subtitle, { x: 0.72, y: 1.82, w: 7.6, h: 0.78 }, {
      maxFontSize: 17,
      minFontSize: 12,
      color: COLORS.muted,
    }));
  }
}

function addChip(slide, text, x, y, w, accent, textColor = COLORS.white) {
  slide.addShape(pptx.ShapeType.roundRect, {
    x,
    y,
    w,
    h: 0.34,
    rectRadius: 0.06,
    line: { color: accent, transparency: 100 },
    fill: { color: accent },
  });
  slide.addText(text, {
    x: x + 0.08,
    y: y + 0.05,
    w: w - 0.16,
    h: 0.14,
    fontFace: FONT,
    fontSize: 10.5,
    bold: true,
    align: "center",
    color: textColor,
    margin: 0,
  });
}

function addCard(slide, cfg) {
  slide.addShape(pptx.ShapeType.roundRect, {
    x: cfg.x,
    y: cfg.y,
    w: cfg.w,
    h: cfg.h,
    rectRadius: 0.06,
    line: { color: cfg.line || cfg.fill, width: 1.2 },
    fill: { color: cfg.fill },
  });
  slide.addText(cfg.title, {
    x: cfg.x + 0.16,
    y: cfg.y + 0.14,
    w: cfg.w - 0.32,
    h: 0.22,
    fontFace: FONT,
    fontSize: 15,
    bold: true,
    color: cfg.titleColor || COLORS.ink,
    margin: 0,
  });
  slide.addText(
    cfg.body,
    fitText(cfg.body, { x: cfg.x + 0.16, y: cfg.y + 0.46, w: cfg.w - 0.32, h: cfg.h - 0.62 }, {
      maxFontSize: cfg.maxFontSize || 16,
      minFontSize: cfg.minFontSize || 11,
      color: cfg.bodyColor || COLORS.muted,
      valign: "top",
    })
  );
}

function addStep(slide, cfg) {
  slide.addShape(pptx.ShapeType.roundRect, {
    x: cfg.x,
    y: cfg.y,
    w: cfg.w,
    h: cfg.h,
    rectRadius: 0.05,
    line: { color: cfg.fill, transparency: 100 },
    fill: { color: cfg.fill },
  });
  slide.addText(cfg.index, {
    x: cfg.x + 0.1,
    y: cfg.y + 0.08,
    w: 0.34,
    h: 0.18,
    fontFace: FONT,
    fontSize: 13,
    bold: true,
    color: COLORS.white,
    margin: 0,
  });
  slide.addText(cfg.title, {
    x: cfg.x + 0.1,
    y: cfg.y + 0.3,
    w: cfg.w - 0.2,
    h: 0.28,
    fontFace: FONT,
    fontSize: 15,
    bold: true,
    color: COLORS.white,
    margin: 0,
  });
  slide.addText(
    cfg.body,
    fitText(cfg.body, { x: cfg.x + 0.1, y: cfg.y + 0.66, w: cfg.w - 0.2, h: cfg.h - 0.82 }, {
      maxFontSize: 13,
      minFontSize: 10,
      color: COLORS.white,
      valign: "top",
    })
  );
}

function finalizeSlide(slide) {
  warnIfSlideHasOverlaps(slide, pptx);
  warnIfSlideElementsOutOfBounds(slide, pptx);
}

function slide1() {
  const slide = pptx.addSlide();
  addCanvas(slide, COLORS.rust, "阿布价格行为学 / 视频要点", 1);
  slide.addText("23A 旗形", {
    x: 0.7,
    y: 0.72,
    w: 2.7,
    h: 0.22,
    fontFace: FONT,
    fontSize: 12,
    bold: true,
    color: COLORS.rust,
    margin: 0,
  });
  slide.addText("最终旗形不是延续的例外，而是趋势后期的警报", {
    x: 0.7,
    y: 0.98,
    w: 7.85,
    h: 0.8,
    fontFace: FONT,
    fontSize: 24,
    bold: true,
    color: COLORS.ink,
    margin: 0,
  });
  slide.addText(
    "核心结论：真正值得交易者警惕的，不是旗形本身，而是旗形在趋势末端如何变成交易区间、如何让突破失败、以及如何把顺势交易切换成反转准备。",
    fitText("核心结论：真正值得交易者警惕的，不是旗形本身，而是旗形在趋势末端如何变成交易区间、如何让突破失败、以及如何把顺势交易切换成反转准备。", {
      x: 0.72,
      y: 1.82,
      w: 7.1,
      h: 0.78,
    }, {
      maxFontSize: 16,
      minFontSize: 12,
      color: COLORS.muted,
    })
  );

  addChip(slide, "趋势后期", 0.74, 2.9, 1.25, COLORS.rust);
  addChip(slide, "交易区间化", 2.1, 2.9, 1.55, COLORS.teal);
  addChip(slide, "突破失败", 3.8, 2.9, 1.35, COLORS.red);

  addCard(slide, {
    x: 0.72,
    y: 3.45,
    w: 3.9,
    h: 2.48,
    fill: COLORS.cream,
    line: COLORS.sand,
    title: "这节课要回答的 3 个问题",
    body: "1. 什么叫“最终旗形”？\n2. 多头与空头的末端迹象分别是什么？\n3. 当突破失败时，交易者应该如何调整节奏？",
    titleColor: COLORS.ink,
    bodyColor: COLORS.ink,
    maxFontSize: 16,
  });

  addCard(slide, {
    x: 4.92,
    y: 3.45,
    w: 3.75,
    h: 2.48,
    fill: "E7EEE7",
    line: COLORS.moss,
    title: "一句话框架",
    body: "普通旗形意味着“回调后大概率延续”；最终旗形意味着“回调已经长成共识区，后续突破更像末端冲刺，失败后容易回到区间并转向”。",
    titleColor: COLORS.ink,
    bodyColor: COLORS.ink,
    maxFontSize: 16,
  });

  addCard(slide, {
    x: 8.95,
    y: 0.92,
    w: 3.62,
    h: 5.02,
    fill: "202D36",
    line: "202D36",
    title: "观察顺序",
    body: "先看趋势是否已经走了很久，再看旗形是否由回调变成交易区间，最后才评估突破是继续还是失败。\n\n顺序错了，就会把末端冲刺误判成新的起涨或起跌。",
    titleColor: COLORS.white,
    bodyColor: "EEF2F4",
    maxFontSize: 16,
  });

  finalizeSlide(slide);
}

function slide2() {
  const slide = pptx.addSlide();
  addCanvas(slide, COLORS.teal, "概念框架 / 普通旗形 vs 最终旗形", 2);
  addTitle(
    slide,
    "概念框架",
    "从普通旗形到最终旗形",
    "一旦旗形长成交易区间，市场就不再只是在“回调”，而是在讨论新的公平价格。",
    COLORS.teal
  );

  addCard(slide, {
    x: 0.78,
    y: 2.7,
    w: 3.55,
    h: 3.95,
    fill: COLORS.cream,
    line: COLORS.sand,
    title: "普通旗形",
    body: "位置：趋势中段\n语义：顺势回调\n交易者默认：突破后继续\n常见外观：小三角形、双顶/双底旗形、I-I-I\n重点：看延续，而不是先看反转",
    bodyColor: COLORS.ink,
    maxFontSize: 15,
  });

  addCard(slide, {
    x: 4.55,
    y: 2.7,
    w: 3.55,
    h: 3.95,
    fill: "F4E7E2",
    line: COLORS.clay,
    title: "最终旗形",
    body: "位置：趋势后期\n语义：最后一个旗形 / 最后的交易区间\n交易者默认：突破不一定可靠，失败后更要重视反转\n常见外观：水平区间、收缩三角形、楔形、双顶更低高点 / 双底更高低点",
    bodyColor: COLORS.ink,
    maxFontSize: 15,
  });

  addStep(slide, {
    x: 8.5,
    y: 2.7,
    w: 1.1,
    h: 3.2,
    fill: COLORS.rust,
    index: "01",
    title: "趋势",
    body: "先有一段已经走出动能的单边行情。",
  });
  slide.addText("→", {
    x: 9.7,
    y: 4.05,
    w: 0.32,
    h: 0.2,
    fontFace: FONT,
    fontSize: 24,
    bold: true,
    color: COLORS.muted,
    margin: 0,
  });
  addStep(slide, {
    x: 10.05,
    y: 2.7,
    w: 1.1,
    h: 3.2,
    fill: COLORS.teal,
    index: "02",
    title: "旗形",
    body: "回调开始时仍像延续，但越走越像区间。",
  });
  slide.addText("→", {
    x: 11.25,
    y: 4.05,
    w: 0.32,
    h: 0.2,
    fontFace: FONT,
    fontSize: 24,
    bold: true,
    color: COLORS.muted,
    margin: 0,
  });
  addStep(slide, {
    x: 11.6,
    y: 2.7,
    w: 1.1,
    h: 3.2,
    fill: COLORS.red,
    index: "03",
    title: "失败",
    body: "突破并不延续，价格重新回到共识区并转向。",
  });

  finalizeSlide(slide);
}

function slide3() {
  const slide = pptx.addSlide();
  addCanvas(slide, COLORS.rust, "多头末端 / 最终多头旗形", 3);
  addTitle(
    slide,
    "多头末端",
    "最终多头旗形：向上突破不再天然值得追",
    "文中的判断关键词不是“有旗形”，而是“已经走了很久、越来越横、离阻力越来越近”。",
    COLORS.rust
  );

  addCard(slide, {
    x: 0.78,
    y: 2.72,
    w: 2.85,
    h: 1.65,
    fill: "F8E7DF",
    line: COLORS.clay,
    title: "趋势已经很老",
    body: "多头趋势持续 20 根或更多 K 线，回调很少，买入高潮开始堆积。",
  });
  addCard(slide, {
    x: 3.92,
    y: 2.72,
    w: 2.85,
    h: 1.65,
    fill: "F7EFE1",
    line: COLORS.gold,
    title: "离阻力太近",
    body: "旗形出现在前高、等距目标、楔形顶点或明显阻力位下方。",
  });
  addCard(slide, {
    x: 7.06,
    y: 2.72,
    w: 2.85,
    h: 1.65,
    fill: "E8F0E8",
    line: COLORS.moss,
    title: "回调已经长成区间",
    body: "原本像回调的旗形，拖成 10 到 20 根左右的横向交易区间。",
  });
  addCard(slide, {
    x: 10.2,
    y: 2.72,
    w: 2.35,
    h: 1.65,
    fill: "E2ECEC",
    line: COLORS.teal,
    title: "突破后还要看",
    body: "有没有真正创新高；有没有立刻失去动能；有没有回到区间。",
    maxFontSize: 14,
  });

  addCard(slide, {
    x: 0.78,
    y: 4.75,
    w: 7.15,
    h: 1.42,
    fill: "202D36",
    line: "202D36",
    title: "交易员动作",
    body: "顺势多头在这种位置要更快止盈；一旦向上突破失败，就要准备寻找更低高点、双顶更低高点、跌回区间下沿等反转卖点。",
    titleColor: COLORS.white,
    bodyColor: "F1F4F5",
    maxFontSize: 16,
  });

  addCard(slide, {
    x: 8.2,
    y: 4.75,
    w: 4.35,
    h: 1.42,
    fill: "F6F1E8",
    line: COLORS.red,
    title: "一句提醒",
    body: "多头趋势总会在阻力结束。末端的水平旗形，往往不是继续加速，而是在问：价格是不是已经太高了？",
    bodyColor: COLORS.ink,
    maxFontSize: 15,
  });

  finalizeSlide(slide);
}

function slide4() {
  const slide = pptx.addSlide();
  addCanvas(slide, COLORS.teal, "空头末端 / 最终空头旗形", 4);
  addTitle(
    slide,
    "空头末端",
    "最终空头旗形：最后一跌经常出现在支撑附近",
    "当空头趋势已经抛售很久、反弹仍然压不出新低时，旗形会从延续结构变成衰竭结构。",
    COLORS.teal
  );

  addCard(slide, {
    x: 0.78,
    y: 2.72,
    w: 2.85,
    h: 1.65,
    fill: "E6F0EF",
    line: COLORS.teal,
    title: "支撑就在脚下",
    body: "空头旗形贴着通道底、等距目标或明显支撑区域出现。",
  });
  addCard(slide, {
    x: 3.92,
    y: 2.72,
    w: 2.85,
    h: 1.65,
    fill: "EAF2E5",
    line: COLORS.moss,
    title: "卖盘已经疲劳",
    body: "抛售高潮、连续大阴线、几乎没有回调，说明空头可能先要获利了结。",
  });
  addCard(slide, {
    x: 7.06,
    y: 2.72,
    w: 2.85,
    h: 1.65,
    fill: "F4EDE2",
    line: COLORS.gold,
    title: "反弹像区间而非新趋势",
    body: "低2、双顶空头旗形、上倾楔形，都是“反弹仍像旗形”的常见外观。",
  });
  addCard(slide, {
    x: 10.2,
    y: 2.72,
    w: 2.35,
    h: 1.65,
    fill: "F6E8E7",
    line: COLORS.red,
    title: "关键不是跌破",
    body: "关键在于跌破后能否真正创新低；若不能，失败突破会很快把多头拉回来。",
    maxFontSize: 14,
  });

  addCard(slide, {
    x: 0.78,
    y: 4.75,
    w: 7.15,
    h: 1.42,
    fill: "1F3C4A",
    line: "1F3C4A",
    title: "交易员动作",
    body: "在空头趋势末端，第一次向上反转常常仍是次要的，但第二次尝试、微双底、楔形底和失败突破后的强力上拉，才是真正值得准备的多头机会。",
    titleColor: COLORS.white,
    bodyColor: "F1F5F7",
    maxFontSize: 15,
  });

  addCard(slide, {
    x: 8.2,
    y: 4.75,
    w: 4.35,
    h: 1.42,
    fill: "F6F1E8",
    line: COLORS.teal,
    title: "一句提醒",
    body: "每个空头趋势都在支撑结束。末端旗形的向下突破，很可能只是最后一脚。",
    bodyColor: COLORS.ink,
    maxFontSize: 15,
  });

  finalizeSlide(slide);
}

function slide5() {
  const slide = pptx.addSlide();
  addCanvas(slide, COLORS.red, "失败突破 / 交易路径", 5);
  addTitle(
    slide,
    "交易路径",
    "失败突破的典型路径",
    "这节课最重要的观察顺序：不是先问“有没有突破”，而是先问“这个突破有没有资格失败”。",
    COLORS.red
  );

  const steps = [
    ["01", "老趋势", "单边行情已经走很久，回调稀少。", COLORS.rust],
    ["02", "旗形成区间", "回调延长成横向共识区，市场开始寻找公平价格。", COLORS.teal],
    ["03", "末端突破", "价格冲出区间，表面像延续，实质更像最后冲刺。", COLORS.gold],
    ["04", "没有新极端", "突破没有真正创新高或创新低，或者立刻回到区间。", COLORS.red],
    ["05", "反转或震荡", "价格回归共识区，再展开反转或进入更大级别交易区间。", COLORS.moss],
  ];

  let x = 0.82;
  steps.forEach((step, idx) => {
    addStep(slide, {
      x,
      y: 2.95,
      w: 2.2,
      h: 1.95,
      fill: step[3],
      index: step[0],
      title: step[1],
      body: step[2],
    });
    x += 2.42;
    if (idx < steps.length - 1) {
      slide.addText("→", {
        x: x - 0.22,
        y: 3.83,
        w: 0.14,
        h: 0.16,
        fontFace: FONT,
        fontSize: 16,
        bold: true,
        color: COLORS.muted,
        margin: 0,
      });
    }
  });

  addCard(slide, {
    x: 0.82,
    y: 5.45,
    w: 12.0,
    h: 0.92,
    fill: COLORS.cream,
    line: COLORS.sand,
    title: "文中反复强调的细节",
    body: "“没有先到达新高/新低就反转”本身就是信息。它意味着突破不是新的趋势开始，而是趋势末端动能已经衰竭。",
    bodyColor: COLORS.ink,
    maxFontSize: 15,
  });

  finalizeSlide(slide);
}

function slide6() {
  const slide = pptx.addSlide();
  addCanvas(slide, COLORS.gold, "结构标签 / 文中高频说法", 6);
  addTitle(
    slide,
    "结构标签",
    "文中高频结构标签，不是名词堆砌",
    "三角形、I-I-I、双顶双底旗形、楔形和交易区间，都是在帮交易者判断：现在到底是在延续，还是在衰竭。",
    COLORS.gold
  );

  const rows = [
    ["小三角形 / I-I-I", "收缩型旗形，常出现在趋势末端。", "首个突破不一定可靠，要看会不会很快回归区间。"],
    ["双顶 / 双底旗形", "二次测试前高前低，说明延续在减弱。", "更低高点 / 更高低点比名词本身更重要。"],
    ["楔形", "三推衰竭，常把趋势推向主要反转。", "越接近阻力或支撑，越要警惕失败突破。"],
    ["水平交易区间", "多空都把这里当公平价格。", "脱离区间后，很多时候会很快被吸回。"],
    ["等距运动目标", "市场会被目标位吸引，也会在目标位犹豫。", "一到目标附近，就先问“还能不能继续”。"],
  ];

  slide.addShape(pptx.ShapeType.roundRect, {
    x: 0.82,
    y: 2.6,
    w: 12.0,
    h: 3.85,
    rectRadius: 0.05,
    line: { color: COLORS.slate, width: 1.2 },
    fill: { color: COLORS.white },
  });
  const colX = [0.98, 3.45, 7.58];
  const colW = [2.25, 3.9, 4.95];
  ["文中说法", "结构语义", "交易提醒"].forEach((head, idx) => {
    slide.addShape(pptx.ShapeType.rect, {
      x: colX[idx],
      y: 2.78,
      w: colW[idx],
      h: 0.46,
      line: { color: COLORS.ink, transparency: 100 },
      fill: { color: COLORS.ink },
    });
    slide.addText(head, {
      x: colX[idx] + 0.08,
      y: 2.92,
      w: colW[idx] - 0.16,
      h: 0.14,
      fontFace: FONT,
      fontSize: 11.5,
      bold: true,
      align: "center",
      color: COLORS.white,
      margin: 0,
    });
  });

  let rowY = 3.34;
  rows.forEach((row, rowIdx) => {
    const fill = rowIdx % 2 === 0 ? COLORS.cream : "FAFBFC";
    slide.addShape(pptx.ShapeType.rect, {
      x: 0.98,
      y: rowY,
      w: 11.55,
      h: 0.56,
      line: { color: COLORS.slate, width: 0.5 },
      fill: { color: fill },
    });
    row.forEach((text, idx) => {
      slide.addText(
        text,
        fitText(text, { x: colX[idx] + 0.08, y: rowY + 0.08, w: colW[idx] - 0.16, h: 0.38 }, {
          maxFontSize: 12,
          minFontSize: 9.8,
          color: COLORS.ink,
          valign: "mid",
        })
      );
    });
    rowY += 0.6;
  });

  finalizeSlide(slide);
}

function slide7() {
  const slide = pptx.addSlide();
  addCanvas(slide, COLORS.moss, "交易员行动清单 / 课后带走什么", 7);
  addTitle(
    slide,
    "行动清单",
    "看见旗形之后，不要急着问“做不做”，先问“它是不是最后一个旗形”",
    "这节课真正训练的，是交易员切换节奏的能力：从顺势延续，切到快速止盈，再切到等待失败突破与反转。",
    COLORS.moss
  );

  addCard(slide, {
    x: 0.82,
    y: 2.72,
    w: 5.7,
    h: 3.75,
    fill: COLORS.cream,
    line: COLORS.sand,
    title: "课后行动清单",
    body: "1. 每次见到旗形，先判断趋势是否已经走老。\n2. 旗形一旦拖成 10 到 20 根左右，就要从“回调”改口成“区间”。\n3. 看到阻力、支撑和等距目标时，不再无脑追突破。\n4. 记录“突破有没有真正创新高/低”。\n5. 对顺势仓位更快止盈，对失败突破准备反向计划。",
    bodyColor: COLORS.ink,
    maxFontSize: 15,
  });

  slide.addShape(pptx.ShapeType.roundRect, {
    x: 6.88,
    y: 2.92,
    w: 5.1,
    h: 3.15,
    rectRadius: 0.08,
    line: { color: "24313A", transparency: 100 },
    fill: { color: "24313A" },
  });
  slide.addText("最后一句总结", {
    x: 7.2,
    y: 3.16,
    w: 2.2,
    h: 0.22,
    fontFace: FONT,
    fontSize: 14,
    bold: true,
    color: COLORS.gold,
    margin: 0,
  });
  slide.addText(
    "最终旗形不是一个新名词，而是一种交易者视角：\n\n当趋势已经很成熟，旗形就不再只是延续的跳板，而更可能是突破失败的起点。",
    fitText("最终旗形不是一个新名词，而是一种交易者视角：\n\n当趋势已经很成熟，旗形就不再只是延续的跳板，而更可能是突破失败的起点。", {
      x: 7.18,
      y: 3.56,
      w: 4.45,
      h: 1.5,
    }, {
      maxFontSize: 18,
      minFontSize: 13,
      color: COLORS.white,
      valign: "mid",
    })
  );

  addChip(slide, "先看位置", 7.18, 5.45, 1.15, COLORS.rust);
  addChip(slide, "再看区间化", 8.5, 5.45, 1.45, COLORS.teal);
  addChip(slide, "最后看失败", 10.15, 5.45, 1.45, COLORS.red);

  finalizeSlide(slide);
}

async function main() {
  slide1();
  slide2();
  slide3();
  slide4();
  slide5();
  slide6();
  slide7();
  await pptx.writeFile({ fileName: OUT, compression: true });
  console.log(`saved: ${OUT}`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
