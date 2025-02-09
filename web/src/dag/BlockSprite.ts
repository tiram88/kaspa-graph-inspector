import * as PIXI from "pixi.js-legacy";
import {Ease, Tween} from "@createjs/tweenjs";
import {Block} from "../model/Block";
import {BlockColor} from "../model/BlockColor";

const blockColors: { [color: string]: number } = {"gray": 0xf5faff, "red": 0xfc606f, "blue": 0xb4cfed};
const highlightColors: { [color: string]: number } = {"gray": 0x78869e, "red": 0x9e4949, "blue": 0x49849e};
const blockRoundingRadius = 10;
const blockTextures: { [key: string]: PIXI.RenderTexture } = {};

const blockTexture = (application: PIXI.Application, blockSize: number): PIXI.RenderTexture => {
    const key = `${blockSize}`
    if (!blockTextures[key]) {
        const graphics = new PIXI.Graphics();
        graphics.lineStyle(3, 0xaaaaaa);
        graphics.beginFill(0xffffff);
        graphics.drawRoundedRect(0, 0, blockSize, blockSize, blockRoundingRadius);
        graphics.endFill();

        blockTextures[key] = application.renderer.generateTexture(graphics, PIXI.SCALE_MODES.LINEAR, 1);
    }

    return blockTextures[key];
};

export default class BlockSprite extends PIXI.Container {
    private readonly unfocusedScale = 0.9;
    private readonly focusedScale = 1.0;
    private readonly textSizeMultiplier = 0.25;

    private readonly application: PIXI.Application;
    private readonly block: Block;
    private readonly spriteContainer: PIXI.Container;
    private readonly textContainer: PIXI.Container;
    private readonly highlightContainer: PIXI.Container;

    private blockSize: number = 0;
    private isBlockSizeInitialized: boolean = false;
    private blockColor: string = BlockColor.GRAY;
    private isHighlighted: boolean = false;
    private highlightColor: string = BlockColor.GRAY;
    private currentSprite: PIXI.Sprite;
    private currentHighlight: PIXI.Graphics;
    private blockClickedListener: (block: Block) => void;

    constructor(application: PIXI.Application, block: Block) {
        super();

        this.application = application;
        this.block = block;

        this.blockClickedListener = () => {
            // Do nothing
        };

        this.spriteContainer = new PIXI.Container();
        this.addChild(this.spriteContainer);

        this.textContainer = new PIXI.Container();
        this.addChild(this.textContainer);

        this.highlightContainer = new PIXI.Container();
        this.highlightContainer.alpha = 0.0;
        this.addChild(this.highlightContainer);

        this.currentSprite = this.buildSprite();
        this.spriteContainer.addChild(this.currentSprite);

        this.currentHighlight = this.buildHighlight();
        this.highlightContainer.addChild(this.currentHighlight);

        this.scale.set(this.unfocusedScale, this.unfocusedScale);
    }

    private buildSprite = (): PIXI.Sprite => {
        const sprite = new PIXI.Sprite();
        sprite.anchor.set(0.5, 0.5);
        sprite.tint = blockColors[this.blockColor];

        sprite.interactive = true;
        sprite.buttonMode = true;
        sprite.on("pointerover", () => {
            Tween.get(this.scale).to({x: this.focusedScale, y: this.focusedScale}, 200, Ease.quadOut);
        });
        sprite.on("pointerout", () => {
            Tween.get(this.scale).to({x: this.unfocusedScale, y: this.unfocusedScale}, 200, Ease.quadOut);
        });
        sprite.on("pointertap", () => this.blockClickedListener(this.block));

        return sprite;
    }

    private buildText = (blockSize: number): PIXI.Text => {
        const style = new PIXI.TextStyle({
            fontFamily: '"Lucida Console", "Courier", monospace',
            fontSize: blockSize * this.textSizeMultiplier,
            fontWeight: "bold",
            fill: 0x666666,
        });

        const blockHashLength = this.block.blockHash.length;
        const lastEightBlockHashCharacters = this.block.blockHash.substring(blockHashLength - 8).toUpperCase();
        const firstFourDisplayCharacters = lastEightBlockHashCharacters.substring(0, 4);
        const lastFourDisplayCharacter = lastEightBlockHashCharacters.substring(4);
        const displayHash = `${firstFourDisplayCharacters}\n${lastFourDisplayCharacter}`;

        const text = new PIXI.Text(displayHash, style);
        text.anchor.set(0.5, 0.5);
        text.tint = blockColors[this.blockColor];
        return text;
    }

    private buildHighlight = (): PIXI.Graphics => {
        const highlightOffset = 11;
        const highlightSize = this.blockSize + highlightOffset;
        const highlightRoundingRadius = blockRoundingRadius + (highlightOffset / 2);

        const graphics = new PIXI.Graphics();
        graphics.lineStyle(5, highlightColors[this.highlightColor]);
        graphics.drawRoundedRect(0, 0, highlightSize, highlightSize, highlightRoundingRadius);
        graphics.position.set(-highlightSize / 2, -highlightSize / 2);
        return graphics;
    }

    setSize = (blockSize: number) => {
        if (!this.currentSprite.texture || this.blockSize !== blockSize) {
            this.blockSize = blockSize;
            this.currentSprite.texture = blockTexture(this.application, blockSize);

            const text = this.buildText(blockSize);
            this.textContainer.removeChildren();
            this.textContainer.addChild(text);

            const highlight = this.buildHighlight();
            this.highlightContainer.removeChildren();
            this.highlightContainer.addChild(highlight);
        }
        this.isBlockSizeInitialized = true;
    }

    wasBlockSizeSet = (): boolean => {
        return this.isBlockSizeInitialized;
    }

    setColor = (color: string) => {
        if (this.blockColor !== color) {
            this.blockColor = color;

            const oldSprite = this.currentSprite;

            this.currentSprite = this.buildSprite();
            this.currentSprite.texture = blockTexture(this.application, this.blockSize);
            this.currentSprite.alpha = 0.0;
            this.spriteContainer.addChild(this.currentSprite);

            Tween.get(this.currentSprite)
                .to({alpha: 1.0}, 500)
                .call(() => this.spriteContainer.removeChild(oldSprite));
        }
    }

    setHighlighted = (isHighlighted: boolean) => {
        if (this.isHighlighted !== isHighlighted) {
            this.isHighlighted = isHighlighted;

            const toAlpha = this.isHighlighted ? 1.0 : 0.0;
            Tween.get(this.highlightContainer)
                .to({alpha: toAlpha}, 300);
        }
    }

    setHighlightColor = (highlightColor: string) => {
        if (this.highlightColor !== highlightColor) {
            this.highlightColor = highlightColor;

            const oldHighlight = this.currentHighlight;

            this.currentHighlight = this.buildHighlight();
            this.currentHighlight.alpha = 0.0;
            this.highlightContainer.addChild(this.currentHighlight);

            Tween.get(this.currentHighlight)
                .to({alpha: 1.0}, 300)
                .call(() => this.highlightContainer.removeChild(oldHighlight));
        }
    }

    setBlockClickedListener = (blockClickedListener: (block: Block) => void) => {
        this.blockClickedListener = blockClickedListener;
    }

    // clampVectorToBounds clamps the given vector's magnitude
    // to be fully within the block's shape
    static clampVectorToBounds = (blockSize: number, vectorX: number, vectorY: number): { blockBoundsVectorX: number, blockBoundsVectorY: number } => {
        const halfBlockSize = blockSize / 2;

        // Don't bother with any fancy calculations if the y
        // coordinate is exactly 0
        if (vectorY === 0) {
            return {
                blockBoundsVectorX: vectorX >= 0 ? halfBlockSize : -halfBlockSize,
                blockBoundsVectorY: 0,
            };
        }

        const halfBlockSizeMinusCorner = halfBlockSize - blockRoundingRadius;

        // Abs the vector's x and y before getting its tangent
        // so that it's a bit easier to reason about
        const tangentOfAngle = Math.abs(vectorY) / Math.abs(vectorX);

        // Is the vector passing through the vertical lines of
        // the block?
        const yForHalfBlockSize = halfBlockSize * tangentOfAngle;
        if (yForHalfBlockSize <= halfBlockSizeMinusCorner) {
            return {
                blockBoundsVectorX: vectorX >= 0 ? halfBlockSize : -halfBlockSize,
                blockBoundsVectorY: vectorY >= 0 ? yForHalfBlockSize : -yForHalfBlockSize,
            };
        }

        // Is the vector passing through the horizontal lines of
        // the block?
        const xForHalfBlockSize = halfBlockSize / tangentOfAngle;
        if (xForHalfBlockSize <= halfBlockSizeMinusCorner) {
            return {
                blockBoundsVectorX: vectorX >= 0 ? xForHalfBlockSize : -xForHalfBlockSize,
                blockBoundsVectorY: vectorY >= 0 ? halfBlockSize : -halfBlockSize
            };
        }

        // If we reached here, the vector is certainly passing
        // through a corner.
        // The following calculation is derived from solving:
        //   (x-m)^2 + (y-n)^2 = r^2
        //   tan(α) = y/x
        // Where:
        //   m and n are `halfBlockSizeMinusCorner`
        //   tan(α) is `tangentOfAngle`
        //   r is `blockRoundingRadius`
        const a = (tangentOfAngle ** 2) + 1;
        const b = -(2 * halfBlockSizeMinusCorner * (tangentOfAngle + 1));
        const c = (2 * (halfBlockSizeMinusCorner ** 2)) - (blockRoundingRadius ** 2);
        const x = (-b + Math.sqrt((b ** 2) - (4 * a * c))) / (2 * a);
        const y = x * tangentOfAngle;

        return {
            blockBoundsVectorX: vectorX >= 0 ? x : -x,
            blockBoundsVectorY: vectorY >= 0 ? y : -y,
        };
    }
};
