import * as PIXI from "pixi.js-legacy";
import {Ease, Tween} from "@createjs/tweenjs";

const heightTextures: { [key: string]: PIXI.RenderTexture } = {};

const heightTexture = (application: PIXI.Application, width: number, height: number): PIXI.RenderTexture => {
    const key = `${width},${height}`
    if (!heightTextures[key]) {
        const graphics = new PIXI.Graphics();
        graphics.beginFill(0xf7f9fa);
        graphics.drawRect(0, 0, width, height);
        graphics.endFill();

        heightTextures[key] = application.renderer.generateTexture(graphics, PIXI.SCALE_MODES.LINEAR, 1);
    }

    return heightTextures[key];
};

export default class HeightSprite extends PIXI.Container {
    private readonly textSizeMultiplier = 0.15;
    private readonly textBottomMarginMultiplier = 0.5;

    private readonly application: PIXI.Application;
    private readonly blockHeight: number;
    private readonly spriteContainer: PIXI.Container;
    private readonly textContainer: PIXI.Container;

    private currentSprite: PIXI.Sprite;

    private spriteWidth: number = 0;
    private spriteHeight: number = 0;
    private heightClickedListener: (height: number) => void;

    constructor(application: PIXI.Application, blockHeight: number) {
        super();

        this.application = application;
        this.blockHeight = blockHeight;

        this.heightClickedListener = () => {
            // Do nothing
        };

        this.spriteContainer = new PIXI.Container();
        this.addChild(this.spriteContainer);

        this.textContainer = new PIXI.Container();
        this.addChild(this.textContainer);

        this.currentSprite = this.buildSprite();
        this.spriteContainer.addChild(this.currentSprite);
    }

    private buildSprite = (): PIXI.Sprite => {
        const sprite = new PIXI.Sprite();
        sprite.anchor.set(0.5, 0.5);
        sprite.alpha = 0.0;

        sprite.interactive = true;
        sprite.buttonMode = true;
        sprite.on("pointerover", () => {
            Tween.get(sprite).to({alpha: 1.0}, 200, Ease.linear);
        });
        sprite.on("pointerout", () => {
            Tween.get(sprite).to({alpha: 0.0}, 200, Ease.linear);
        });
        sprite.on("pointertap", () => this.heightClickedListener(this.blockHeight));

        return sprite;
    }

    private buildText = (spriteHeight: number, blockSize: number): PIXI.Text => {
        const style = new PIXI.TextStyle({
            fontFamily: '"Verdana", "Arial", "Helvetica", sans-serif',
            fontSize: blockSize * this.textSizeMultiplier,
            fill: 0xffffff,
        });

        const text = new PIXI.Text(this.blockHeight.toLocaleString("en-US"), style);
        text.anchor.set(0.5, 0.5);
        text.tint = 0x777777;
        text.y = (spriteHeight / 2) - (blockSize * this.textBottomMarginMultiplier);
        return text;
    }

    setSize = (width: number, height: number, blockSize: number) => {
        if (!this.currentSprite.texture || this.spriteWidth !== width || this.spriteHeight !== height) {
            this.spriteWidth = width;
            this.spriteHeight = height;
            this.currentSprite.texture = heightTexture(this.application, width, height);

            const text = this.buildText(height, blockSize);
            this.textContainer.removeChildren();
            this.textContainer.addChild(text);
        }
    }

    getHeight = (): number => {
        return this.blockHeight;
    }

    setHeightClickedListener = (heightClickedListener: (height: number) => void) => {
        this.heightClickedListener = heightClickedListener;
    }
}