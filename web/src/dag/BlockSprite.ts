import * as PIXI from "pixi.js-legacy";
import { Ease, Tween } from "@createjs/tweenjs";
import { Block } from "../model/Block";
import { BlockColorConst, BlockColor } from "../model/BlockColor";
import { HighlightFrame, theme } from "./Theme";
import { chunkSubstr } from "../common/tools";

//const blockColors: { [color: string]: number } = {"gray": 0xf5faff, "red": 0xfc606f, "blue": 0xb4cfed};
//const highlightColors: { [color: string]: number } = {"gray": 0x78869e, "red": 0x9e4949, "blue": 0x49849e};
//const blockRoundingRadius = 10;
const blockTextures: { [key: string]: PIXI.RenderTexture } = {};

const blockTexture = (application: PIXI.Application, blockSize: number, blockColor: BlockColor): PIXI.RenderTexture => {
    const resolution = application.renderer.resolution;
    const key = `${blockSize}-${blockColor}-${resolution}`
    if (!blockTextures[key]) {
        const graphics = new PIXI.Graphics();
        graphics.lineStyle(theme.scale(theme.components.block[blockColor].border.width, blockSize), theme.components.block[blockColor].border.color, 1, 0.5);
        graphics.beginFill(0xffffff);
        graphics.drawRoundedRect(0, 0, blockSize, blockSize, theme.scale(theme.components.block.roundingRadius, blockSize));
        graphics.endFill();

        let textureOptions: PIXI.IGenerateTextureOptions  = {
            scaleMode: PIXI.SCALE_MODES.LINEAR,
            resolution: resolution,
        }
        blockTextures[key] = application.renderer.generateTexture(graphics, textureOptions);
    }

    return blockTextures[key];
};

export default class BlockSprite extends PIXI.Container {
    private readonly application: PIXI.Application;
    private readonly block: Block;
    private readonly spriteContainer: PIXI.Container;
    private readonly textContainer: PIXI.Container;
    private readonly highlightContainer: PIXI.Container;

    private blockSize: number = 0;
    private isBlockSizeInitialized: boolean = false;
    private blockColor:  BlockColor = BlockColorConst.GRAY;
    private hasFocus: boolean = false;
    private isHighlighted: boolean = false;
    private highlightColor: BlockColor = BlockColorConst.GRAY;
    private currentSprite: PIXI.Sprite;
    private currentText?: PIXI.Text;
    private currentHighlight: PIXI.Graphics;
    private blockClickedListener: (block: Block) => void;
    // Guards destroy() itself against being run twice, and guards the deferred
    // .call(() => oldX.destroy()) callbacks below against firing on a child
    // that was already destroyed as part of a whole-sprite destroy() - see the
    // long comment on destroy() at the bottom of this file for why that
    // combination matters. Public so callers that keep their own deferred
    // work referencing this sprite (e.g. TimelineContainer's position-tween
    // onChange handlers) can check it too, rather than only guarding what
    // this class schedules internally.
    public isDestroyed: boolean = false;

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

        this.scale.set(theme.components.block.scale.default, theme.components.block.scale.default);
    }

    private buildSprite = (): PIXI.Sprite => {
        const sprite = new PIXI.Sprite();
        sprite.anchor.set(0.5, 0.5);
        sprite.tint = theme.components.block[this.blockColor].color.main;

        sprite.interactive = true;
        sprite.buttonMode = true;
        sprite.on("pointerover", () => {
            Tween.get(this.scale).to({x: theme.components.block.scale.hover, y: theme.components.block.scale.hover}, 200, Ease.quadOut);
        });
        sprite.on("pointerout", () => {
            Tween.get(this.scale).to({x: theme.components.block.scale.default, y: theme.components.block.scale.default}, 200, Ease.quadOut);
        });
        sprite.on("pointertap", () => this.blockClickedListener(this.block));

        return sprite;
    }

    private buildText = (blockSize: number): PIXI.Text => {
        const nominalFontSize = blockSize * theme.components.block.text.multiplier.size * 2;
        const textLines = nominalFontSize <= theme.components.block.text.minFontSize - 2 ? Math.max(
            1, 
            Math.min(Math.floor(((nominalFontSize) / theme.components.block.text.minFontSize)),
            theme.components.block.text.maxTextLines)) : Math.max(
            1, 
            Math.min(Math.ceil(((nominalFontSize + 2) / theme.components.block.text.maxFontSize)),
            theme.components.block.text.maxTextLines));
        const style = new PIXI.TextStyle({
            fontFamily: theme.components.block.text.fontFamily,
            fontSize: Math.floor(Math.min(nominalFontSize / textLines, theme.components.block.text.maxFontSize) * 4.0) / 4.0,
            fontWeight: theme.components.block.text.fontWeight,
            fill: theme.components.block[this.blockColor].color.contrastText,
        });

        const blockHashLength = this.block.blockHash.length;
        const lineLength = textLines * 2;
        const lastCharactersLength = (lineLength * textLines);
        const lastBlockHashCharacters = this.block.blockHash.substring(blockHashLength - lastCharactersLength).toUpperCase();
        let displayHash = chunkSubstr(lastBlockHashCharacters, lineLength).join('\n');
        const text = new PIXI.Text(displayHash, style);

        text.anchor.set(0.5, 0.5);
        return text;
    }

    private buildHighlight = (): PIXI.Graphics => {
        const blockHighlight = this.getHighlightFrame();
        const highlightSize = this.blockSize + theme.scale(blockHighlight.offset, this.blockSize);
        const highlightRoundingRadius = theme.scale(theme.components.block.roundingRadius + (blockHighlight.offset / 2), this.blockSize);

        const graphics = new PIXI.Graphics();
        graphics.lineStyle(theme.scale(blockHighlight.lineWidth, this.blockSize), theme.components.block[this.highlightColor].color.highlight);
        graphics.drawRoundedRect(0, 0, highlightSize, highlightSize, highlightRoundingRadius);
        graphics.position.set(-highlightSize / 2, -highlightSize / 2);
        return graphics;
    }

    setSize = (blockSize: number) => {
        if (!this.currentSprite.texture || this.blockSize !== blockSize) {
            this.blockSize = blockSize;
            this.currentSprite.texture = blockTexture(this.application, blockSize, this.blockColor);

            // setColor()/setHighlighted() may currently have a cross-fade in
            // flight: a Tween targeting this.currentText/this.currentHighlight
            // whose completion callback destroy()s the respective *previous*
            // text/highlight (see below). We're about to hard-reset both
            // containers unconditionally regardless of that, destroying
            // everything currently in them - old and mid-fade "current" alike.
            // If we don't cancel those tweens first, their .call() fires later
            // and tries to destroy() an object this method already destroyed,
            // which throws and corrupts the shared CreateJS tween loop (the
            // exact bug reported against the previous version of this fix).
            if (this.currentText) {
                Tween.removeTweens(this.currentText);
            }
            Tween.removeTweens(this.currentHighlight);

            this.currentText = this.buildText(blockSize);
            // removeChildren() only detaches the previous PIXI.Text from the display
            // tree - it does not free the canvas/GPU texture backing it. Explicitly
            // destroy() every removed child (each PIXI.Text owns its own texture, so
            // this is always safe here) or it leaks for the lifetime of the tab.
            this.textContainer.removeChildren().forEach(child => child.destroy());
            this.textContainer.addChild(this.currentText);

            this.currentHighlight = this.buildHighlight();
            this.highlightContainer.removeChildren().forEach(child => child.destroy());
            this.highlightContainer.addChild(this.currentHighlight);
        }
        this.isBlockSizeInitialized = true;
    }

    wasBlockSizeSet = (): boolean => {
        return this.isBlockSizeInitialized;
    }

    setColor = (color: BlockColor) => {
        if (this.blockColor !== color) {
            this.blockColor = color;

            const oldSprite = this.currentSprite;

            this.currentSprite = this.buildSprite();
            this.currentSprite.texture = blockTexture(this.application, this.blockSize, this.blockColor);
            this.currentSprite.alpha = 0.0;
            this.spriteContainer.addChild(this.currentSprite);

            const oldText = this.currentText;
            this.currentText = this.buildText(this.blockSize);
            if (!oldText) {
                this.textContainer.removeChildren().forEach(child => child.destroy());
                this.textContainer.addChild(this.currentText);
            } else {
                this.currentText.alpha = 0.0;
                this.textContainer.addChild(this.currentText);
                Tween.get(this.currentText)
                    .to({alpha: 1.0}, 300)
                    // destroy(), not removeChild(): oldText's canvas texture is its
                    // own (never shared), so it must be freed once it's off screen.
                    //
                    // Guarded by isDestroyed: if this whole BlockSprite gets
                    // destroy()'d (e.g. it scrolled out of view) before this
                    // 300ms fade finishes, oldText was already destroyed as
                    // part of that cascade - destroying it again here would
                    // throw (PIXI destroy() is not safe to call twice).
                    .call(() => {
                        if (!this.isDestroyed) {
                            // Belt-and-suspenders: isDestroyed and the
                            // removeTweens() calls in setSize()/destroy()
                            // account for every path we've found that could
                            // destroy oldText before this callback runs, but
                            // a stray double-destroy() throwing here would
                            // otherwise corrupt the shared CreateJS tween
                            // loop for every other animation in the app, so
                            // this is deliberately over-cautious.
                            try {
                                oldText!.destroy();
                            } catch (e) {
                                console.warn("BlockSprite: failed to destroy oldText", e);
                            }
                        }
                    });
            }

            Tween.get(this.currentSprite)
                .to({alpha: 1.0}, 500)
                // oldSprite's texture comes from the shared blockTextures cache, so
                // the default destroy() (texture: false) removes it from the
                // display tree and frees its own resources without touching the
                // cached texture that other BlockSprites still rely on.
                //
                // Guarded by isDestroyed for the same reason as oldText above.
                .call(() => {
                    if (!this.isDestroyed) {
                        try {
                            oldSprite.destroy();
                        } catch (e) {
                            console.warn("BlockSprite: failed to destroy oldSprite", e);
                        }
                    }
                });
        }
    }

    setHighlighted = (isHighlighted: boolean, hasFocus: boolean, highlightColor: BlockColor) => {
        if (this.isHighlighted !== isHighlighted || this.hasFocus !== hasFocus || this.highlightColor !== highlightColor) {
            this.isHighlighted = isHighlighted;
            this.hasFocus = hasFocus;
            this.highlightColor = highlightColor;
            const blockHighlight = this.getHighlightFrame();

            const oldHighlight = this.currentHighlight;

            if (oldHighlight.alpha > 0.0 && this.highlightContainer.alpha > 0.0) {
                Tween.get(oldHighlight)
                .to({alpha: 0.0}, 300)
                // Guarded by isDestroyed - see the matching comment in setColor().
                .call(() => {
                    if (!this.isDestroyed) {
                        try {
                            oldHighlight.destroy();
                        } catch (e) {
                            console.warn("BlockSprite: failed to destroy oldHighlight", e);
                        }
                    }
                });
            } else {
                this.highlightContainer.removeChildren().forEach(child => child.destroy());
            }

            this.currentHighlight = this.buildHighlight();
            this.currentHighlight.alpha = 0.0;
            this.highlightContainer.addChild(this.currentHighlight);

            if (isHighlighted) {
                Tween.get(this.currentHighlight)
                .to({alpha: 1.0}, 300);
            }
 
            const toAlpha = this.isHighlighted ? blockHighlight.alpha : 0.0;
            if (toAlpha !== this.highlightContainer.alpha) {
                Tween.get(this.highlightContainer)
                .to({alpha: toAlpha}, 300);
            }
        }
    }

    setBlockClickedListener = (blockClickedListener: (block: Block) => void) => {
        this.blockClickedListener = blockClickedListener;
    }

    // destroy() frees every PIXI resource this BlockSprite owns. Callers (e.g.
    // TimelineContainer, when a block scrolls out of the visible range) must call
    // this instead of merely removeChild()-ing the sprite, or its Text/Graphics
    // canvases and event listeners are never released.
    //
    // Idempotency (the isDestroyed guard) matters here specifically because
    // setColor()/setHighlighted() schedule their own deferred destroy() calls
    // on the *previous* sprite/text/graphics via a Tween .call() callback, to
    // let the cross-fade finish first. If the whole BlockSprite is destroyed
    // while one of those fades is still in flight - entirely normal while
    // tracking the tip, where a block can change color right before it
    // scrolls out of the visible range - super.destroy({children: true})
    // below already destroys that old child as part of the cascade. The
    // deferred callback firing afterwards would then destroy() it a second
    // time, which PIXI does not support (it throws) and, left unguarded,
    // silently corrupts the shared CreateJS tween loop that also drives every
    // fade-in and position animation - which is what caused the freeze/
    // disappearing-blocks bug in the previous version of this fix.
    //
    // { children: true } cascades into spriteContainer/textContainer/
    // highlightContainer and on into their children. We deliberately never pass a
    // `texture`/`baseTexture` option here: each PIXI class's own default handles
    // it correctly - PIXI.Sprite defaults to NOT destroying its texture (so the
    // shared, cached blockTexture(...) used by currentSprite survives), while
    // PIXI.Text and PIXI.Graphics default to destroying their own, never-shared
    // resources.
    destroy = (): void => {
        if (this.isDestroyed) {
            return;
        }
        this.isDestroyed = true;
        // TimelineContainer also animates this sprite's position directly
        // (Tween.get(blockSprite).to({y: targetY}, 500, ...)) for smooth
        // reflow when the DAG changes. If that animation is still in flight
        // when this sprite is destroyed, CreateJS would otherwise keep
        // writing to `.y` every frame - which throws once destroy() below
        // has nulled out this object's transform. removeTweens(this) cancels
        // any tween whose target is this exact object (also covers the
        // alpha fade-in tween used when the block is first added), so that
        // can't happen. The pointerover/pointerout hover-scale tween targets
        // this.scale specifically (a separate object), so it needs its own
        // removeTweens() call.
        Tween.removeTweens(this);
        Tween.removeTweens(this.scale);
        super.destroy({ children: true });
    }

    private getHighlightFrame = (): HighlightFrame => {
        return this.hasFocus ? theme.components.block.focus : theme.components.block.highlight;
    }

    // getRealBlockSize returns the actual block size based on
    // a theoretical block size, taking into account the theme properties
    static getRealBlockSize = (blockSize: number): number => {
        // As we have no knowledge of an actual block, we base the calculation
        // on theme blue block, considered the most relevant
        return (blockSize + theme.scale(theme.components.block.blue.border.width / 2.0, blockSize)) * theme.components.block.scale.default;
    }

    // clampVectorToBounds clamps the given vector's magnitude
    // to be fully within the block's shape
    static clampVectorToBounds = (blockSize: number, vectorX: number, vectorY: number): { blockBoundsVectorX: number, blockBoundsVectorY: number } => {
        const realBlockSize = BlockSprite.getRealBlockSize(blockSize);
        const halfBlockSize = realBlockSize / 2;

        // Don't bother with any fancy calculations if the y
        // coordinate is exactly 0
        if (vectorY === 0) {
            return {
                blockBoundsVectorX: vectorX >= 0 ? halfBlockSize : -halfBlockSize,
                blockBoundsVectorY: 0,
            };
        }

        const roundingRadius = theme.scale(theme.components.block.roundingRadius, blockSize)
        const halfBlockSizeMinusCorner = halfBlockSize - roundingRadius;

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
        //   r is `roundingRadius`
        const a = (tangentOfAngle ** 2) + 1;
        const b = -(2 * halfBlockSizeMinusCorner * (tangentOfAngle + 1));
        const c = (2 * (halfBlockSizeMinusCorner ** 2)) - (roundingRadius ** 2);
        const x = (-b + Math.sqrt((b ** 2) - (4 * a * c))) / (2 * a);
        const y = x * tangentOfAngle;

        return {
            blockBoundsVectorX: vectorX >= 0 ? x : -x,
            blockBoundsVectorY: vectorY >= 0 ? y : -y,
        };
    }
};
